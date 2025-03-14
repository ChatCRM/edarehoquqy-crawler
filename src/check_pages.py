from aiopath import AsyncPath
import aiofiles
import asyncio
import logging
from typing import Set, List, Dict, Any, Optional, Tuple
import os
import json
from datetime import datetime

from src.handler import LegalOpinionsCrawler, FileProcessor
from src._core.schemas import CrawlerConfig, IdeaPageInfo, CustomSearchParams, ResultItem
from src.elasticsearch_service import ElasticsearchService
from src.processor import Processor, ProcessingTask
from src.transfer.file_manager import FileManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("check_pages.log")
    ]
)

logger = logging.getLogger(__name__)


class MissingPagesChecker:
    """Checks for missing pages in Elasticsearch and processes them."""
    
    def __init__(
        self,
        output_dir: str = "../output",
        elasticsearch_index: str = "edarehoquqy",
        num_parser_workers: int = 50,
        num_embedding_workers: int = 50,
        num_indexing_workers: int = 50,
        max_concurrent_requests: int = 5,
        request_delay: float = 0.5,
    ):
        """
        Initialize the missing pages checker.
        
        Args:
            output_dir: Path to the output directory
            elasticsearch_index: Name of the Elasticsearch index
            num_parser_workers: Number of parser workers
            num_embedding_workers: Number of embedding workers
            num_indexing_workers: Number of indexing workers
            max_concurrent_requests: Maximum number of concurrent requests
            request_delay: Delay between requests
        """
        self.output_dir = output_dir
        self.elasticsearch_index = elasticsearch_index
        self.num_parser_workers = num_parser_workers
        self.num_embedding_workers = num_embedding_workers
        self.num_indexing_workers = num_indexing_workers
        self.max_concurrent_requests = max_concurrent_requests
        self.request_delay = request_delay
        
        # Initialize services
        self.elasticsearch_service = ElasticsearchService(index_name=elasticsearch_index)
        self.file_manager = FileManager(output_dir=output_dir)
        
        # Initialize crawler
        base_path = AsyncPath(output_dir)
        self.file_processor = FileProcessor(base_path)
        self.crawler_config = CrawlerConfig(
            max_concurrent_requests=max_concurrent_requests,
            request_delay=request_delay,
            base_path=base_path
        )
        self.crawler = LegalOpinionsCrawler(self.crawler_config, self.file_processor)
        
        # Stats
        self.stats = {
            "total_ids": 0,
            "indexed_ids": 0,
            "missing_ids": 0,
            "processed_ids": 0,
            "failed_ids": 0,
            "start_time": None,
            "end_time": None
        }
    
    async def extract_ids_from_html_file(self, html_file: AsyncPath) -> Set[str]:
        """
        Extract IDs from an HTML file.
        
        Args:
            html_file: Path to the HTML file
            
        Returns:
            Set[str]: Set of extracted IDs
        """
        from bs4 import BeautifulSoup
        import re
        
        extracted_ids = set()
        
        try:
            # Read the HTML content
            async with aiofiles.open(html_file, "r", encoding="utf-8") as f:
                html_content = await f.read()
            
            # Parse the HTML to extract IDs
            soup = BeautifulSoup(html_content, "html.parser")
            
            # Try to extract IDs from the HTML
            # First, try to parse as JSON response
            try:
                # Look for JSON data in the HTML
                json_match = re.search(r'(\{.*"results":\s*\[.*\].*\})', html_content)
                if json_match:
                    json_data = json.loads(json_match.group(1))
                    if "results" in json_data:
                        for result in json_data["results"]:
                            if "IdeaId" in result and result["IdeaId"]:
                                extracted_ids.add(str(result["IdeaId"]))
                            elif "DocumentUrl" in result and result["DocumentUrl"]:
                                extracted_ids.add(result["DocumentUrl"])
            except Exception as e:
                logger.debug(f"Failed to parse JSON from {html_file.name}: {str(e)}")
            
            # If no IDs found via JSON, try to extract from HTML structure
            if not extracted_ids:
                # Look for links that might contain IDs
                links = soup.find_all("a", href=True)
                for link in links:
                    href = link.get("href", "")
                    # Extract IdeaId from URL
                    id_match = re.search(r'IdeaId=(\d+)', href)
                    if id_match:
                        extracted_ids.add(id_match.group(1))
        
        except Exception as e:
            logger.error(f"Error processing main page file {html_file.name}: {str(e)}")
        
        return extracted_ids
    
    async def get_all_ids_from_main_pages(self) -> Set[str]:
        """
        Get all IDs from the already crawled main pages in ../output/main_pages.
        
        Returns:
            Set[str]: Set of all IDs
        """
        logger.info("Getting all IDs from already crawled main pages")
        all_ids = set()
        
        # Initialize the file processor
        await self.file_processor.initialize()
        
        # Path to main pages directory
        main_pages_path = AsyncPath(self.output_dir) / "main_pages"
        
        if not await main_pages_path.exists():
            logger.error(f"Main pages directory {main_pages_path} does not exist")
            return all_ids
        
        # Get all HTML files in the main_pages directory
        html_files = [f for f in await main_pages_path.glob("*.html")]
        logger.info(f"Found {len(html_files)} main page HTML files")
        
        # Process each HTML file to extract IDs
        for html_file in html_files:
            file_ids = await self.extract_ids_from_html_file(html_file)
            all_ids.update(file_ids)
            
            # Log progress periodically
            if len(all_ids) % 1000 == 0:
                logger.info(f"Extracted {len(all_ids)} IDs so far")
        
        # If we couldn't extract IDs from HTML files, fall back to crawling
        if not all_ids:
            logger.warning("Could not extract IDs from HTML files, falling back to crawling")
            return await self._crawl_main_pages_for_ids()
        
        logger.info(f"Found {len(all_ids)} total IDs from main pages")
        self.stats["total_ids"] = len(all_ids)
        return all_ids
    
    async def _crawl_main_pages_for_ids(self) -> Set[str]:
        """
        Fallback method to crawl main pages for IDs if we can't extract them from files.
        
        Returns:
            Set[str]: Set of all IDs
        """
        logger.info("Falling back to crawling main pages for IDs")
        all_ids = set()
        
        # Initialize the crawler session
        if not await self.crawler.initialize_session():
            logger.error("Failed to initialize crawler session")
            return all_ids
        
        # Use the crawler's crawl_all_results method to get all results
        try:
            results = await self.crawler.crawl_all_results()
            
            # Extract IDs from results
            for result in results:
                if hasattr(result, 'IdeaId') and result.IdeaId:
                    all_ids.add(str(result.IdeaId))
                elif hasattr(result, 'DocumentUrl') and result.DocumentUrl:
                    all_ids.add(result.DocumentUrl)
            
            logger.info(f"Found {len(all_ids)} total IDs from crawling")
            self.stats["total_ids"] = len(all_ids)
            return all_ids
            
        except Exception as e:
            logger.error(f"Error crawling main pages: {str(e)}")
            return all_ids
    
    async def get_indexed_ids_from_elasticsearch(self) -> Set[str]:
        """
        Get all IDs that are already indexed in Elasticsearch.
        
        Returns:
            Set[str]: Set of indexed IDs
        """
        logger.info("Getting indexed IDs from Elasticsearch")
        indexed_ids = set()
        
        try:
            # Ensure the index exists
            await self.elasticsearch_service.ensure_index_exists()
            
            # Query for all document IDs
            query = {
                "query": {
                    "match_all": {}
                },
                "_source": False,
                "size": 10000  # Adjust based on your index size
            }
            
            # Use the scroll API for large indices
            response = await self.elasticsearch_service.client.search(
                index=self.elasticsearch_index,
                body=query,
                scroll="2m"
            )
            
            scroll_id = response["_scroll_id"]
            hits = response["hits"]["hits"]
            
            # Process initial batch
            for hit in hits:
                indexed_ids.add(hit["_id"])
            
            # Continue scrolling until no more results
            while len(hits) > 0:
                response = await self.elasticsearch_service.client.scroll(
                    scroll_id=scroll_id,
                    scroll="2m"
                )
                
                scroll_id = response["_scroll_id"]
                hits = response["hits"]["hits"]
                
                for hit in hits:
                    indexed_ids.add(hit["_id"])
            
            logger.info(f"Found {len(indexed_ids)} indexed IDs in Elasticsearch")
            self.stats["indexed_ids"] = len(indexed_ids)
            return indexed_ids
            
        except Exception as e:
            logger.error(f"Error getting indexed IDs from Elasticsearch: {str(e)}")
            return indexed_ids
    
    async def process_missing_ids(self, missing_ids: Set[str]) -> None:
        """
        Process missing IDs.
        
        Args:
            missing_ids: Set of missing IDs
        """
        if not missing_ids:
            logger.info("No missing IDs to process")
            return
        
        logger.info(f"Processing {len(missing_ids)} missing IDs")
        self.stats["missing_ids"] = len(missing_ids)
        
        # Initialize the processor
        processor = Processor(
            output_dir=self.output_dir,
            num_parser_workers=self.num_parser_workers,
            num_embedding_workers=self.num_embedding_workers,
            num_indexing_workers=self.num_indexing_workers,
            elasticsearch_index=self.elasticsearch_index
        )
        
        # Initialize the file manager
        await self.file_manager.initialize()
        
        # Start workers
        parser_tasks = await processor.start_parser_workers()
        embedding_tasks = await processor.start_embedding_workers()
        indexing_tasks = await processor.start_indexing_workers()
        
        # Process each missing ID
        processed_count = 0
        failed_count = 0
        
        # First, check if the HTML files already exist in the pages directory
        existing_files = []
        missing_files = []
        
        for idea_id in missing_ids:
            idea_dir = self.file_processor.pages_path / str(idea_id)
            html_file = idea_dir / f"{idea_id}.html"
            
            if await AsyncPath(html_file).exists():
                existing_files.append((idea_id, html_file))
            else:
                missing_files.append(idea_id)
        
        logger.info(f"Found {len(existing_files)} existing HTML files and {len(missing_files)} missing files")
        
        # Process existing files first
        for idea_id, html_file in existing_files:
            try:
                # Read the HTML content
                async with aiofiles.open(html_file, "r", encoding="utf-8") as f:
                    html_content = await f.read()
                
                # Create a processing task
                task = ProcessingTask(idea_id, str(html_file), html_content)
                
                # Add the task to the parser queue
                await processor.parser_queue.put(task)
                processed_count += 1
                
                # Log progress
                if processed_count % 100 == 0:
                    logger.info(f"Queued {processed_count} existing files for processing")
                
            except Exception as e:
                logger.error(f"Error processing existing file for ID {idea_id}: {str(e)}")
                failed_count += 1
        
        # Initialize the crawler session if we need to fetch missing files
        if missing_files:
            logger.info(f"Initializing crawler session to fetch {len(missing_files)} missing files")
            if not await self.crawler.initialize_session():
                logger.error("Failed to initialize crawler session, skipping missing files")
                for idea_id in missing_files:
                    failed_count += 1
                missing_files = []
        
        # Process missing files
        for idea_id in missing_files:
            try:
                # Fetch the HTML content
                html_content = await self.crawler.get_idea_page(idea_id)
                
                if not html_content:
                    logger.error(f"Failed to fetch HTML content for ID {idea_id}")
                    failed_count += 1
                    continue
                
                # Save the HTML content
                idea_dir = self.file_processor.pages_path / str(idea_id)
                html_file = idea_dir / f"{idea_id}.html"
                await idea_dir.mkdir(parents=True, exist_ok=True)
                async with aiofiles.open(html_file, "w", encoding="utf-8") as f:
                    await f.write(html_content)
                
                # Create a processing task
                task = ProcessingTask(idea_id, str(html_file), html_content)
                
                # Add the task to the parser queue
                await processor.parser_queue.put(task)
                processed_count += 1
                
                # Log progress
                if processed_count % 100 == 0:
                    logger.info(f"Queued {processed_count}/{len(missing_ids)} IDs for processing")
                
                # Add a small delay between requests
                await asyncio.sleep(self.request_delay)
                
            except Exception as e:
                logger.error(f"Error processing ID {idea_id}: {str(e)}")
                failed_count += 1
        
        # Wait for all queues to be processed
        logger.info("Waiting for all queued tasks to complete")
        await processor.parser_queue.join()
        
        # Wait for any remaining batches to be processed
        await asyncio.sleep(5)
        
        # Flush any remaining documents in Elasticsearch
        await processor.elasticsearch_service.flush_all()
        
        # Stop workers
        processor.stop_event.set()
        
        # Cancel worker tasks
        for task in parser_tasks + embedding_tasks + indexing_tasks:
            task.cancel()
        
        # Wait for worker tasks to complete
        await asyncio.gather(*parser_tasks, *embedding_tasks, *indexing_tasks, return_exceptions=True)
        
        # Update stats
        self.stats["processed_ids"] = processed_count
        self.stats["failed_ids"] = failed_count
        
        logger.info(f"Processed {processed_count} missing IDs, failed {failed_count} IDs")
    
    async def check_and_process(self) -> Dict[str, Any]:
        """
        Check for missing pages and process them.
        
        Returns:
            Dict[str, Any]: Processing statistics
        """
        self.stats["start_time"] = datetime.now()
        
        try:
            # Get all IDs from main pages
            all_ids = await self.get_all_ids_from_main_pages()
            
            # Get indexed IDs from Elasticsearch
            indexed_ids = await self.get_indexed_ids_from_elasticsearch()
            
            # Find missing IDs
            missing_ids = all_ids - indexed_ids
            
            # Process missing IDs
            await self.process_missing_ids(missing_ids)
            
            self.stats["end_time"] = datetime.now()
            processing_time = (self.stats["end_time"] - self.stats["start_time"]).total_seconds()
            self.stats["processing_time_seconds"] = processing_time
            
            logger.info(f"Processing completed in {processing_time:.2f} seconds")
            logger.info(f"Total IDs: {self.stats['total_ids']}")
            logger.info(f"Indexed IDs: {self.stats['indexed_ids']}")
            logger.info(f"Missing IDs: {self.stats['missing_ids']}")
            logger.info(f"Processed IDs: {self.stats['processed_ids']}")
            logger.info(f"Failed IDs: {self.stats['failed_ids']}")
            
            return self.stats
        except Exception as e:
            logger.error(f"Error checking and processing missing pages: {str(e)}")
            self.stats["error"] = str(e)
            raise
        finally:
            # Close connections
            await self.elasticsearch_service.close()
            await self.crawler.close()


async def main():
    """Main entry point for the script."""
    # Configuration variables with hardcoded values
    output_dir = "../output"
    elasticsearch_index = "edarehoquqy"
    num_parser_workers = 50
    num_embedding_workers = 50
    num_indexing_workers = 50
    max_concurrent_requests = 5
    request_delay = 0.5
    
    # Create the checker
    checker = MissingPagesChecker(
        output_dir=output_dir,
        elasticsearch_index=elasticsearch_index,
        num_parser_workers=num_parser_workers,
        num_embedding_workers=num_embedding_workers,
        num_indexing_workers=num_indexing_workers,
        max_concurrent_requests=max_concurrent_requests,
        request_delay=request_delay
    )
    
    # Check and process missing pages
    stats = await checker.check_and_process()
    
    # Save stats to file
    stats_file = AsyncPath(output_dir) / "check_pages_stats.json"
    async with aiofiles.open(stats_file, "w", encoding="utf-8") as f:
        await f.write(json.dumps(stats, default=str, indent=2))
    
    logger.info(f"Stats saved to {stats_file}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
    except Exception as e:
        logger.error(f"Error running checker: {str(e)}")
        raise



