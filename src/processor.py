import os
import asyncio
import logging
from typing import Dict, Any, List, Optional, Tuple, Set
from aiopath import AsyncPath
import time
from datetime import datetime
import aiofiles

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
    
    def qsize(self) -> int:
        """Get the current size of the queue."""
        return self.queue.qsize()
    
    def empty(self) -> bool:
        """Check if the queue is empty."""
        return self.queue.empty()


class BatchProcessor:
    """Processor for batch operations."""
    
    def __init__(self, batch_size: int = 10, max_wait_time: float = 2.0):
        """
        Initialize a batch processor.
        
        Args:
            batch_size: Maximum batch size
            max_wait_time: Maximum time to wait for a batch to fill up (in seconds)
        """
        self.batch_size = batch_size
        self.max_wait_time = max_wait_time
        self.batch = []
        self.batch_event = asyncio.Event()
        self.lock = asyncio.Lock()
        self.last_item_time = time.time()
    
    async def add_item(self, item: Any) -> None:
        """
        Add an item to the batch.
        
        Args:
            item: Item to add
        """
        async with self.lock:
            self.batch.append(item)
            self.last_item_time = time.time()
            
            if len(self.batch) >= self.batch_size:
                self.batch_event.set()
    
    async def get_batch(self) -> List[Any]:
        """
        Get the current batch and reset.
        
        Returns:
            List[Any]: Current batch
        """
        async with self.lock:
            batch = self.batch
            self.batch = []
            self.batch_event.clear()
            return batch
    
    async def wait_for_batch(self) -> List[Any]:
        """
        Wait for a batch to be ready.
        
        Returns:
            List[Any]: Ready batch
        """
        while True:
            # Check if batch is ready
            if len(self.batch) >= self.batch_size:
                return await self.get_batch()
            
            # Check if we've waited long enough
            time_since_last_item = time.time() - self.last_item_time
            if self.batch and time_since_last_item >= self.max_wait_time:
                return await self.get_batch()
            
            # Wait for batch event or timeout
            try:
                wait_time = max(0.1, min(self.max_wait_time - time_since_last_item, self.max_wait_time))
                await asyncio.wait_for(self.batch_event.wait(), timeout=wait_time)
            except asyncio.TimeoutError:
                # Timeout, check batch again
                pass


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
        embedding_batch_size: int = 8,
        indexing_batch_size: int = 50,
        max_batch_wait_time: float = 2.0,
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
            embedding_batch_size: Size of embedding batches
            indexing_batch_size: Size of indexing batches
            max_batch_wait_time: Maximum time to wait for a batch to fill up
        """
        self.file_manager = FileManager(output_dir=output_dir)
        self.parser = HTMLParser()
        self.embedding_service = EmbeddingService(batch_size=embedding_batch_size)
        self.elasticsearch_service = ElasticsearchService(
            index_name=elasticsearch_index,
            bulk_size=indexing_batch_size
        )
        
        self.num_parser_workers = num_parser_workers
        self.num_embedding_workers = num_embedding_workers
        self.num_indexing_workers = num_indexing_workers
        
        self.parser_queue = ProcessingQueue(maxsize=parser_queue_size)
        self.embedding_queue = ProcessingQueue(maxsize=embedding_queue_size)
        self.indexing_queue = ProcessingQueue(maxsize=indexing_queue_size)
        
        self.metadata = metadata or {}
        
        self.embedding_batch_processor = BatchProcessor(
            batch_size=embedding_batch_size,
            max_wait_time=max_batch_wait_time
        )
        
        self.indexing_batch_processor = BatchProcessor(
            batch_size=indexing_batch_size,
            max_wait_time=max_batch_wait_time
        )
        
        self.stop_event = asyncio.Event()
        self.processed_files = set()
        self.failed_ids = set()  # Set to track failed IDs
        self.output_dir = output_dir
        self.stats = {
            "total_files": 0,
            "parsed_files": 0,
            "embedded_files": 0,
            "indexed_files": 0,
            "error_files": 0,
            "start_time": None,
            "end_time": None
        }
    
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
                    # Skip if already processed
                    if task.file_id in self.processed_files:
                        logger.info(f"Skipping already processed file {task.file_id}")
                        self.parser_queue.task_done()
                        await self.parser_queue.mark_completed(success=True)
                        continue
                    
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
                    
                    # Add to embedding batch processor
                    await self.embedding_batch_processor.add_item(task)
                    
                    # Mark as processed
                    self.processed_files.add(task.file_id)
                    self.stats["parsed_files"] += 1
                    
                    await self.parser_queue.mark_completed(success=True)
                except Exception as e:
                    logger.error(f"Error parsing HTML content for file {task.file_id}: {str(e)}")
                    task.error = f"Parsing error: {str(e)}"
                    self.stats["error_files"] += 1
                    self.failed_ids.add(task.file_id)  # Add to failed IDs
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
                # Wait for a batch of tasks
                batch = await self.embedding_batch_processor.wait_for_batch()
                
                if not batch:
                    # No tasks in batch, wait a bit
                    await asyncio.sleep(0.1)
                    continue
                
                try:
                    # Prepare texts for batch embedding
                    tasks_with_content = []
                    texts = []
                    
                    for task in batch:
                        if task.parsed_content is None:
                            logger.warning(f"Task {task.file_id} has no parsed content, skipping")
                            self.failed_ids.add(task.file_id)  # Add to failed IDs
                            continue
                        
                        tasks_with_content.append(task)
                        texts.append(task.parsed_content.content)
                    
                    if not texts:
                        logger.warning("No valid texts for embedding")
                        continue
                    
                    # Generate embeddings in batch
                    embeddings = await self.embedding_service.process_texts_in_batches(texts)
                    
                    # Update tasks with embeddings
                    for i, task in enumerate(tasks_with_content):
                        if i < len(embeddings):
                            task.embedding = embeddings[i]
                            
                            # Add to indexing batch processor
                            await self.indexing_batch_processor.add_item(task)
                            
                            # Update stats
                            self.stats["embedded_files"] += 1
                        else:
                            logger.error(f"Missing embedding for task {task.file_id}")
                            task.error = "Embedding error: Missing embedding in batch result"
                            self.stats["error_files"] += 1
                            self.failed_ids.add(task.file_id)  # Add to failed IDs
                except Exception as e:
                    logger.error(f"Error generating embeddings for batch: {str(e)}")
                    for task in batch:
                        task.error = f"Embedding error: {str(e)}"
                        self.stats["error_files"] += 1
                        self.failed_ids.add(task.file_id)  # Add to failed IDs
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
                # Wait for a batch of tasks
                batch = await self.indexing_batch_processor.wait_for_batch()
                
                if not batch:
                    # No tasks in batch, wait a bit
                    await asyncio.sleep(0.1)
                    continue
                
                try:
                    # Prepare documents for bulk indexing
                    documents = []
                    
                    for task in batch:
                        if task.parsed_content is None or task.embedding is None:
                            logger.warning(f"Task {task.file_id} missing parsed content or embedding, skipping")
                            self.failed_ids.add(task.file_id)  # Add to failed IDs
                            continue
                        
                        documents.append((task.file_id, task.parsed_content, task.embedding))
                    
                    if not documents:
                        logger.warning("No valid documents for indexing")
                        continue
                    
                    # Bulk index documents
                    success = await self.elasticsearch_service.bulk_index_documents(documents)
                    
                    # Update tasks
                    for task in batch:
                        task.indexed = success
                        task.completed_at = datetime.now()
                    
                    # Update stats
                    if success:
                        self.stats["indexed_files"] += len(documents)
                        logger.info(f"Successfully indexed {len(documents)} documents")
                    else:
                        logger.error(f"Failed to index {len(documents)} documents")
                        self.stats["error_files"] += len(documents)
                        # Add all document IDs to failed_ids
                        for doc_id, _, _ in documents:
                            self.failed_ids.add(doc_id)
                except Exception as e:
                    logger.error(f"Error indexing documents: {str(e)}")
                    for task in batch:
                        task.error = f"Indexing error: {str(e)}"
                        self.stats["error_files"] += 1
                        self.failed_ids.add(task.file_id)  # Add to failed IDs
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Unexpected error in indexing worker {worker_id}: {str(e)}")
        
        logger.info(f"Indexing worker {worker_id} stopped")
    
    async def write_failed_ids_to_file(self) -> None:
        """
        Write failed IDs to a file.
        """
        if not self.failed_ids:
            logger.info("No failed IDs to write")
            return
        
        failed_ids_file = AsyncPath(self.output_dir) / "failed_ids.txt"
        logger.info(f"Writing {len(self.failed_ids)} failed IDs to {failed_ids_file}")
        
        try:
            async with await failed_ids_file.open("w", encoding="utf-8") as f:
                for file_id in sorted(self.failed_ids):
                    await f.write(f"{file_id}\n")
            
            logger.info(f"Successfully wrote failed IDs to {failed_ids_file}")
        except Exception as e:
            logger.error(f"Error writing failed IDs to file: {str(e)}")
    
    async def process_files(self) -> Dict[str, Any]:
        """
        Process files from the output directory.
        
        Returns:
            Dict[str, Any]: Processing statistics
        """
        try:
            self.stats["start_time"] = datetime.now()
            
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
                self.stats["total_files"] += 1
            
            # Wait for all queues to be processed
            await self.parser_queue.join()
            
            # Wait for any remaining batches to be processed
            # Give some time for tasks to move through the pipeline
            await asyncio.sleep(5)
            
            # Flush any remaining documents in Elasticsearch
            await self.elasticsearch_service.flush_all()
            
            # Stop workers
            self.stop_event.set()
            
            # Cancel worker tasks
            for task in parser_tasks + embedding_tasks + indexing_tasks:
                task.cancel()
            
            # Wait for worker tasks to complete
            await asyncio.gather(*parser_tasks, *embedding_tasks, *indexing_tasks, return_exceptions=True)
            
            # Write failed IDs to file
            await self.write_failed_ids_to_file()
            
            self.stats["end_time"] = datetime.now()
            processing_time = (self.stats["end_time"] - self.stats["start_time"]).total_seconds()
            self.stats["processing_time_seconds"] = processing_time
            self.stats["failed_ids_count"] = len(self.failed_ids)
            
            logger.info(f"Processing completed in {processing_time:.2f} seconds")
            logger.info(f"Total files: {self.stats['total_files']}")
            logger.info(f"Parsed files: {self.stats['parsed_files']}")
            logger.info(f"Embedded files: {self.stats['embedded_files']}")
            logger.info(f"Indexed files: {self.stats['indexed_files']}")
            logger.info(f"Error files: {self.stats['error_files']}")
            logger.info(f"Failed IDs: {len(self.failed_ids)}")
            
            return self.stats
        except Exception as e:
            logger.error(f"Error processing files: {str(e)}")
            self.stats["error"] = str(e)
            # Try to write failed IDs even if there's an error
            await self.write_failed_ids_to_file()
            raise 