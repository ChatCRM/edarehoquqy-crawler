import os
import asyncio
import logging
from typing import Dict, Any, List, Optional, Tuple
from pathlib import Path
import time
from datetime import datetime
import json

from src.transfer.file_manager import FileManager
from src.parser import HTMLParser, ParsedContent
from src.embedding import EmbeddingService
from src.elasticsearch_service import ElasticsearchService

logger = logging.getLogger(__name__)


class ProcessingTask:
    """Represents a task in the processing pipeline."""
    
    def __init__(self, file_id: str, file_path: str, html_content: str):
        """
        Initialize a processing task.
        
        Args:
            file_id: ID of the file
            file_path: Path to the file
            html_content: HTML content of the file
        """
        self.file_id = file_id
        self.file_path = file_path
        self.html_content = html_content
        self.parsed_content: Optional[ParsedContent] = None
        self.embedding: Optional[List[float]] = None
        self.indexed: bool = False
        self.error: Optional[str] = None
        self.created_at = datetime.now()
        self.completed_at: Optional[datetime] = None


class ProcessingQueue:
    """Queue for processing tasks."""
    
    def __init__(self, maxsize: int = 0):
        """
        Initialize a processing queue.
        
        Args:
            maxsize: Maximum size of the queue (0 for unlimited)
        """
        self.queue = asyncio.Queue(maxsize=maxsize)
        self.processing_count = 0
        self.completed_count = 0
        self.error_count = 0
        self._lock = asyncio.Lock()
    
    async def put(self, task: ProcessingTask) -> None:
        """
        Put a task in the queue.
        
        Args:
            task: Task to put in the queue
        """
        await self.queue.put(task)
        async with self._lock:
            self.processing_count += 1
    
    async def get(self) -> ProcessingTask:
        """
        Get a task from the queue.
        
        Returns:
            ProcessingTask: Task from the queue
        """
        return await self.queue.get()
    
    def task_done(self) -> None:
        """Mark a task as done."""
        self.queue.task_done()
    
    async def mark_completed(self, success: bool = True) -> None:
        """
        Mark a task as completed.
        
        Args:
            success: Whether the task completed successfully
        """
        async with self._lock:
            self.processing_count -= 1
            if success:
                self.completed_count += 1
            else:
                self.error_count += 1
    
    async def join(self) -> None:
        """Wait for all tasks to be processed."""
        await self.queue.join()


class Processor:
    """Main processor that orchestrates the entire workflow."""
    
    def __init__(
        self,
        output_dir: str = "../output",
        num_parser_workers: int = 5,
        num_embedding_workers: int = 3,
        num_indexing_workers: int = 5,
        parser_queue_size: int = 100,
        embedding_queue_size: int = 100,
        indexing_queue_size: int = 100,
        elasticsearch_index: str = "parsed_content",
        metadata: Dict[str, Any] = None,
    ):
        """
        Initialize the processor.
        
        Args:
            output_dir: Path to the output directory
            num_parser_workers: Number of parser workers
            num_embedding_workers: Number of embedding workers
            num_indexing_workers: Number of indexing workers
            parser_queue_size: Size of the parser queue
            embedding_queue_size: Size of the embedding queue
            indexing_queue_size: Size of the indexing queue
            elasticsearch_index: Name of the Elasticsearch index
            metadata: Additional metadata to include in parsed content
        """
        self.file_manager = FileManager(output_dir=output_dir)
        self.parser = HTMLParser()
        self.embedding_service = EmbeddingService()
        self.elasticsearch_service = ElasticsearchService(index_name=elasticsearch_index)
        
        self.num_parser_workers = num_parser_workers
        self.num_embedding_workers = num_embedding_workers
        self.num_indexing_workers = num_indexing_workers
        
        self.parser_queue = ProcessingQueue(maxsize=parser_queue_size)
        self.embedding_queue = ProcessingQueue(maxsize=embedding_queue_size)
        self.indexing_queue = ProcessingQueue(maxsize=indexing_queue_size)
        
        self.metadata = metadata or {}
        
        self.stop_event = asyncio.Event()
    
    async def start_parser_workers(self) -> List[asyncio.Task]:
        """
        Start parser workers.
        
        Returns:
            List[asyncio.Task]: List of worker tasks
        """
        tasks = []
        for i in range(self.num_parser_workers):
            task = asyncio.create_task(self.parser_worker(i))
            tasks.append(task)
        return tasks
    
    async def start_embedding_workers(self) -> List[asyncio.Task]:
        """
        Start embedding workers.
        
        Returns:
            List[asyncio.Task]: List of worker tasks
        """
        tasks = []
        for i in range(self.num_embedding_workers):
            task = asyncio.create_task(self.embedding_worker(i))
            tasks.append(task)
        return tasks
    
    async def start_indexing_workers(self) -> List[asyncio.Task]:
        """
        Start indexing workers.
        
        Returns:
            List[asyncio.Task]: List of worker tasks
        """
        tasks = []
        for i in range(self.num_indexing_workers):
            task = asyncio.create_task(self.indexing_worker(i))
            tasks.append(task)
        return tasks
    
    async def parser_worker(self, worker_id: int) -> None:
        """
        Worker for parsing HTML content.
        
        Args:
            worker_id: ID of the worker
        """
        logger.info(f"Parser worker {worker_id} started")
        
        while not self.stop_event.is_set():
            try:
                task = await self.parser_queue.get()
                
                try:
                    # Add metadata to the task
                    task_metadata = self.metadata.copy()
                    task_metadata.update({
                        "file_id": task.file_id,
                        "file_path": task.file_path,
                        "processed_at": datetime.now().isoformat()
                    })
                    
                    # Parse HTML content
                    parsed_content = await self.parser.parse(task.html_content, task.file_id)
                    
                    # Update metadata
                    parsed_content.metadata.update(task_metadata)
                    
                    # Update task
                    task.parsed_content = parsed_content
                    
                    # Put task in embedding queue
                    await self.embedding_queue.put(task)
                    
                    await self.parser_queue.mark_completed(success=True)
                except Exception as e:
                    logger.error(f"Error parsing HTML content for file {task.file_id}: {str(e)}")
                    task.error = f"Parsing error: {str(e)}"
                    await self.parser_queue.mark_completed(success=False)
                finally:
                    self.parser_queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Unexpected error in parser worker {worker_id}: {str(e)}")
        
        logger.info(f"Parser worker {worker_id} stopped")
    
    async def embedding_worker(self, worker_id: int) -> None:
        """
        Worker for generating embeddings.
        
        Args:
            worker_id: ID of the worker
        """
        logger.info(f"Embedding worker {worker_id} started")
        
        while not self.stop_event.is_set():
            try:
                task = await self.embedding_queue.get()
                
                try:
                    if task.parsed_content is None:
                        raise ValueError("Parsed content is None")
                    
                    # Generate embedding
                    embedding = await self.embedding_service.get_embedding(task.parsed_content.content)
                    
                    # Update task
                    task.embedding = embedding
                    
                    # Put task in indexing queue
                    await self.indexing_queue.put(task)
                    
                    await self.embedding_queue.mark_completed(success=True)
                except Exception as e:
                    logger.error(f"Error generating embedding for file {task.file_id}: {str(e)}")
                    task.error = f"Embedding error: {str(e)}"
                    await self.embedding_queue.mark_completed(success=False)
                finally:
                    self.embedding_queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Unexpected error in embedding worker {worker_id}: {str(e)}")
        
        logger.info(f"Embedding worker {worker_id} stopped")
    
    async def indexing_worker(self, worker_id: int) -> None:
        """
        Worker for indexing documents in Elasticsearch.
        
        Args:
            worker_id: ID of the worker
        """
        logger.info(f"Indexing worker {worker_id} started")
        
        while not self.stop_event.is_set():
            try:
                task = await self.indexing_queue.get()
                
                try:
                    if task.parsed_content is None:
                        raise ValueError("Parsed content is None")
                    
                    if task.embedding is None:
                        raise ValueError("Embedding is None")
                    
                    # Index document
                    success = await self.elasticsearch_service.index_document(
                        task.file_id,
                        task.parsed_content,
                        task.embedding
                    )
                    
                    # Update task
                    task.indexed = success
                    task.completed_at = datetime.now()
                    
                    if success:
                        logger.info(f"Successfully processed file {task.file_id}")
                    else:
                        logger.error(f"Failed to index document for file {task.file_id}")
                        task.error = "Indexing error"
                    
                    await self.indexing_queue.mark_completed(success=success)
                except Exception as e:
                    logger.error(f"Error indexing document for file {task.file_id}: {str(e)}")
                    task.error = f"Indexing error: {str(e)}"
                    await self.indexing_queue.mark_completed(success=False)
                finally:
                    self.indexing_queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Unexpected error in indexing worker {worker_id}: {str(e)}")
        
        logger.info(f"Indexing worker {worker_id} stopped")
    
    async def process_files(self) -> None:
        """Process files from the output directory."""
        try:
            # Ensure Elasticsearch index exists
            await self.elasticsearch_service.ensure_index_exists()
            
            # Start workers
            parser_tasks = await self.start_parser_workers()
            embedding_tasks = await self.start_embedding_workers()
            indexing_tasks = await self.start_indexing_workers()
            
            # Scan directories and put tasks in parser queue
            async for file_id, file_path, html_content in self.file_manager.scan_directories():
                task = ProcessingTask(file_id, file_path, html_content)
                await self.parser_queue.put(task)
            
            # Wait for all queues to be processed
            await self.parser_queue.join()
            await self.embedding_queue.join()
            await self.indexing_queue.join()
            
            # Stop workers
            self.stop_event.set()
            
            # Cancel worker tasks
            for task in parser_tasks + embedding_tasks + indexing_tasks:
                task.cancel()
            
            # Wait for worker tasks to complete
            await asyncio.gather(*parser_tasks, *embedding_tasks, *indexing_tasks, return_exceptions=True)
            
            logger.info("Processing completed")
        except Exception as e:
            logger.error(f"Error processing files: {str(e)}")
            raise 