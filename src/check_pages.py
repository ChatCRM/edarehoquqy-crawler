from aiopath import AsyncPath
import aiofiles
import asyncio
import logging
from typing import Set, List, Dict, Any, Optional, Tuple
import os
import json
from datetime import datetime
from bs4 import BeautifulSoup
import re
from dotenv import load_dotenv


from src.handler import LegalOpinionsCrawler, FileProcessor
from src._core.schemas import CrawlerConfig, IdeaPageInfo, CustomSearchParams, ResultItem
from src.elasticsearch_service import ElasticsearchService
from src.processor import Processor, ProcessingTask
from src.transfer.file_manager import FileManager

load_dotenv()

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
        self.elasticsearch_service = ElasticsearchService(
            index_name=elasticsearch_index, 
            hosts=os.getenv("ELASTIC_SEARCH_HOST"), 
            username=os.getenv("ELASTIC_SEARCH_USERNAME"), 
            password=os.getenv("ELASTIC_SEARCH_PASSWORD")
        )
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
        
        extracted_ids = set()
        
        try:
            # Read the HTML content
            async with aiofiles.open(html_file, "r", encoding="utf-8") as f:
                html_content = await f.read()
            
            # Parse the HTML to extract IDs
            soup = BeautifulSoup(html_content, "html.parser")
            
            # Try to extract IDs from the HTML
            # First, try to parse as JSON response
            json_ids_found = 0
            try:
                # Look for JSON data in the HTML
                json_match = re.search(r'(\{.*"results":\s*\[.*\].*\})', html_content)
                if json_match:
                    logger.debug(f"Found JSON data in {str(html_file).split('/')[-1]}")
                    json_data = json.loads(json_match.group(1))
                    if "results" in json_data:
                        for result in json_data["results"]:
                            if "IdeaId" in result and result["IdeaId"]:
                                extracted_ids.add(str(result["IdeaId"]))
                                json_ids_found += 1
                            elif "DocumentUrl" in result and result["DocumentUrl"]:
                                extracted_ids.add(result["DocumentUrl"])
                                json_ids_found += 1
                    logger.debug(f"Extracted {json_ids_found} IDs from JSON in {str(html_file).split('/')[-1]}")
            except Exception as e:
                logger.debug(f"Failed to parse JSON from {str(html_file).split('/')[-1]}: {str(e)}")
            
            # If no IDs found via JSON, try to extract from HTML structure
            html_ids_found = 0
            if not extracted_ids:
                logger.debug(f"No IDs found in JSON, trying HTML structure for {str(html_file).split('/')[-1]}")
                # Look for links that might contain IDs
                links = soup.find_all("a", href=True)
                logger.debug(f"Found {len(links)} links in {str(html_file).split('/')[-1]}")
                
                for link in links:
                    href = link.get("href", "")
                    # Extract IdeaId from URL
                    id_match = re.search(r'IdeaId=(\d+)', href)
                    if id_match:
                        extracted_ids.add(id_match.group(1))
                        html_ids_found += 1
                
                logger.debug(f"Extracted {html_ids_found} IDs from HTML links in {str(html_file).split('/')[-1]}")
            
            # Try another approach if still no IDs found
            if not extracted_ids:
                logger.debug(f"No IDs found in HTML links, trying direct text search for {str(html_file).split('/')[-1]}")
                # Look for IDs in the text
                id_matches = re.findall(r'IdeaId[=:]\s*[\'"]?(\d+)[\'"]?', html_content)
                for id_match in id_matches:
                    extracted_ids.add(id_match)
                
                logger.debug(f"Extracted {len(id_matches)} IDs from direct text search in {str(html_file).split('/')[-1]}")
        
        except Exception as e:
            logger.error(f"Error processing main page file {str(html_file).split('/')[-1]}: {str(e)}")
        
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
        main_pages_path = AsyncPath(self.output_dir)
        
        if not await main_pages_path.exists():
            logger.error(f"Main pages directory {main_pages_path} does not exist")
            return all_ids
        
        # Get all HTML files in the main_pages directory
        html_files = []
        pattern = "page_*.html"
        logger.info(f"Searching for HTML files with pattern: {pattern}")
        
        async for f in main_pages_path.glob(pattern):
            html_files.append(f)
        
        logger.info(f"Found {len(html_files)} main page HTML files")
        
        # Sort files by page number to process them in order
        html_files.sort(key=lambda x: int(str(x).split('/')[-1].split('_')[1].split('.')[0]))
        
        # Process each HTML file to extract IDs
        processed_files = 0
        total_files = len(html_files)
        
        for html_file in html_files:
            try:
                logger.info(f"Processing file {processed_files+1}/{total_files}: {str(html_file).split('/')[-1]}")
                file_ids = await self.extract_ids_from_html_file(html_file)
                
                logger.info(f"Extracted {len(file_ids)} IDs from {str(html_file).split('/')[-1]}")
                all_ids.update(file_ids)
                processed_files += 1
                
                # Log progress periodically
                if processed_files % 100 == 0:
                    logger.info(f"Processed {processed_files}/{total_files} files, extracted {len(all_ids)} IDs so far")
            except Exception as e:
                logger.error(f"Error processing file {str(html_file).split('/')[-1]}: {str(e)}")
        
        # If we couldn't extract IDs from HTML files, just return empty set instead of crawling
        if not all_ids:
            logger.warning("Could not extract IDs from HTML files. No IDs found to process.")
            return all_ids
        
        logger.info(f"Completed processing {processed_files}/{total_files} files")
        logger.info(f"Found {len(all_ids)} total IDs from {processed_files} main pages")
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
            logger.warning("Continuing without Elasticsearch connection. Will process all IDs.")
            self.stats["indexed_ids"] = 0
            return indexed_ids
    
    async def process_missing_ids(self, missing_ids: Set[str]) -> Set[str]:
        """
        Process missing IDs.
        
        Args:
            missing_ids: Set of missing IDs
            
        Returns:
            Set[str]: Set of failed IDs
        """
        if not missing_ids:
            logger.info("No missing IDs to process")
            return set()
        
        logger.info(f"Processing {len(missing_ids)} missing IDs")
        self.stats["missing_ids"] = len(missing_ids)
        
        # Get Elasticsearch connection parameters from environment variables
        es_host = os.getenv("ELASTIC_SEARCH_HOST") or os.getenv("ES_URL")
        es_username = os.getenv("ELASTIC_SEARCH_USERNAME")
        es_password = os.getenv("ELASTIC_SEARCH_PASSWORD")
        
        # Initialize the processor
        try:
            processor = Processor(
                output_dir=self.output_dir,
                num_parser_workers=self.num_parser_workers,
                num_embedding_workers=self.num_embedding_workers,
                num_indexing_workers=self.num_indexing_workers,
                elasticsearch_index=self.elasticsearch_index,
                es_host=es_host,
                es_username=es_username,
                es_password=es_password
            )
        except Exception as e:
            logger.error(f"Failed to initialize processor: {str(e)}")
            logger.warning("Will continue with file extraction only, without Elasticsearch indexing")
            processor = None
        
        # Initialize the file manager
        try:
            await self.file_manager.initialize()
        except Exception as e:
            logger.error(f"Failed to initialize file manager: {str(e)}")
            return missing_ids  # Return all IDs as failed
        
        # Start workers if processor is available
        parser_tasks = []
        embedding_tasks = []
        indexing_tasks = []
        
        if processor:
            try:
                parser_tasks = await processor.start_parser_workers()
                embedding_tasks = await processor.start_embedding_workers()
                indexing_tasks = await processor.start_indexing_workers()
            except Exception as e:
                logger.error(f"Failed to start processor workers: {str(e)}")
                logger.warning("Will continue with file extraction only, without processing")
                processor = None
        
        # Process each missing ID
        processed_count = 0
        failed_count = 0
        failed_ids = set()  # Track which specific IDs failed
        
        # First, check if the HTML files already exist in the pages directory
        existing_files = []
        missing_files = []
        
        for idea_id in missing_ids:
            try:
                idea_dir = self.file_processor.pages_path / str(idea_id)
                html_file = idea_dir / f"{idea_id}.html"
                
                # Use exists() instead of directly accessing stat()
                if await html_file.exists():
                    existing_files.append((idea_id, html_file))
                else:
                    missing_files.append(idea_id)
            except Exception as e:
                logger.error(f"Error checking file existence for ID {idea_id}: {str(e)}")
                missing_files.append(idea_id)
        
        logger.info(f"Found {len(existing_files)} existing HTML files and {len(missing_files)} missing files")
        
        # Process existing files first
        for idea_id, html_file in existing_files:
            try:
                # Read the HTML content
                async with aiofiles.open(html_file, "r", encoding="utf-8") as f:
                    html_content = await f.read()
                
                # If processor is available, add to queue for processing
                if processor:
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
                failed_ids.add(idea_id)  # Add to failed IDs set
        
        # Initialize the crawler session if we need to fetch missing files
        if missing_files:
            logger.info(f"Initializing crawler session to fetch {len(missing_files)} missing files")
            try:
                if not await self.crawler.initialize_session():
                    logger.error("Failed to initialize crawler session, skipping missing files")
                    for idea_id in missing_files:
                        failed_count += 1
                        failed_ids.add(idea_id)  # Add to failed IDs set
                    missing_files = []
            except Exception as e:
                logger.error(f"Error initializing crawler session: {str(e)}")
                for idea_id in missing_files:
                    failed_count += 1
                    failed_ids.add(idea_id)  # Add to failed IDs set
                missing_files = []
        
        # Process missing files
        for idea_id in missing_files:
            try:
                # Fetch the HTML content
                html_content = await self.crawler.get_idea_page(idea_id)
                
                if not html_content:
                    logger.error(f"Failed to fetch HTML content for ID {idea_id}")
                    failed_count += 1
                    failed_ids.add(idea_id)  # Add to failed IDs set
                    continue
                
                # Save the HTML content
                idea_dir = self.file_processor.pages_path / str(idea_id)
                html_file = idea_dir / f"{idea_id}.html"
                await idea_dir.mkdir(parents=True, exist_ok=True)
                async with aiofiles.open(html_file, "w", encoding="utf-8") as f:
                    await f.write(html_content)
                
                # If processor is available, add to queue for processing
                if processor:
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
                failed_ids.add(idea_id)  # Add to failed IDs set
        
        # Wait for all queues to be processed if processor is available
        if processor:
            try:
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
            except Exception as e:
                logger.error(f"Error waiting for processor tasks to complete: {str(e)}")
        
        # Update stats
        self.stats["processed_ids"] = processed_count
        self.stats["failed_ids"] = failed_count
        
        logger.info(f"Processed {processed_count} missing IDs, failed {failed_count} IDs")
        
        # Return the set of failed IDs
        return failed_ids
    
    async def save_failed_ids(self, failed_ids: Set[str], filename: str = "new_failed_ids.txt") -> None:
        """
        Save failed IDs to a file.
        
        Args:
            failed_ids: Set of failed IDs
            filename: Name of the file to save to
        """
        if not failed_ids:
            logger.info("No failed IDs to save")
            return
            
        file_path = AsyncPath(self.output_dir) / filename
        try:
            async with aiofiles.open(file_path, "w", encoding="utf-8") as f:
                for idea_id in sorted(failed_ids):
                    await f.write(f"{idea_id}\n")
            logger.info(f"Saved {len(failed_ids)} failed IDs to {file_path}")
        except Exception as e:
            logger.error(f"Error saving failed IDs to {file_path}: {str(e)}")
    
    async def check_and_process(self) -> Dict[str, Any]:
        """
        Check for missing pages and process them.
        
        Returns:
            Dict[str, Any]: Processing statistics
        """
        self.stats["start_time"] = datetime.now()
        failed_ids_first_phase = set()
        failed_ids_second_phase = set()
        elasticsearch_available = True
        
        try:
            # PHASE 1: Process existing files
            logger.info("=== PHASE 1: Processing existing files ===")
            
            # Get all IDs from main pages
            all_ids_first_phase = await self.get_all_ids_from_main_pages()
            
            # Get indexed IDs from Elasticsearch
            try:
                indexed_ids_first_phase = await self.get_indexed_ids_from_elasticsearch()
                if not indexed_ids_first_phase:
                    logger.warning("No indexed IDs found in Elasticsearch or connection failed.")
                    elasticsearch_available = False
            except Exception as e:
                logger.error(f"Failed to connect to Elasticsearch: {str(e)}")
                logger.warning("Continuing without Elasticsearch. Will process all IDs.")
                indexed_ids_first_phase = set()
                elasticsearch_available = False
            
            # Find missing IDs
            if elasticsearch_available:
                missing_ids_first_phase = all_ids_first_phase - indexed_ids_first_phase
                logger.info(f"Found {len(missing_ids_first_phase)} missing IDs out of {len(all_ids_first_phase)} total IDs")
            else:
                # If Elasticsearch is not available, process all IDs
                missing_ids_first_phase = all_ids_first_phase
                logger.info(f"Elasticsearch not available. Processing all {len(all_ids_first_phase)} IDs")
            
            # Process missing IDs
            if missing_ids_first_phase:
                logger.info(f"Processing {len(missing_ids_first_phase)} missing IDs in Phase 1")
                
                # Store original failed count
                original_failed_count = self.stats.get("failed_ids", 0)
                
                # Process the missing IDs
                failed_ids_first_phase = await self.process_missing_ids(missing_ids_first_phase)
                
                # Calculate failed IDs from this phase
                new_failed_count = self.stats.get("failed_ids", 0) - original_failed_count
                
                # Log the number of failed IDs
                logger.info(f"Phase 1: Failed to process {len(failed_ids_first_phase)} IDs")
                
                # Save failed IDs from first phase
                await self.save_failed_ids(failed_ids_first_phase, "failed_ids_phase1.txt")
            else:
                logger.info("No missing IDs found in Phase 1")
            
            # PHASE 2: Verification phase - only if Elasticsearch is available
            if elasticsearch_available:
                logger.info("=== PHASE 2: Verification phase ===")
                
                # Crawl main pages again to get all IDs
                logger.info("Crawling main pages again to verify all IDs are processed")
                all_ids_second_phase = await self._crawl_main_pages_for_ids()
                
                # Get updated indexed IDs from Elasticsearch
                try:
                    indexed_ids_second_phase = await self.get_indexed_ids_from_elasticsearch()
                except Exception as e:
                    logger.error(f"Failed to connect to Elasticsearch in Phase 2: {str(e)}")
                    logger.warning("Skipping Phase 2 verification.")
                    indexed_ids_second_phase = set()
                    all_ids_second_phase = set()
                
                # Find any IDs that are still missing
                missing_ids_second_phase = all_ids_second_phase - indexed_ids_second_phase
                
                # Remove IDs that failed in the first phase (we already know they failed)
                missing_ids_second_phase = missing_ids_second_phase - failed_ids_first_phase
                
                # Process any newly discovered missing IDs
                if missing_ids_second_phase:
                    logger.info(f"Found {len(missing_ids_second_phase)} additional missing IDs in Phase 2")
                    
                    # Store original stats to calculate differences
                    original_processed = self.stats.get("processed_ids", 0)
                    original_failed = self.stats.get("failed_ids", 0)
                    
                    # Process the newly discovered missing IDs
                    failed_ids_second_phase = await self.process_missing_ids(missing_ids_second_phase)
                    
                    # Calculate new failures from this phase
                    new_failed = self.stats.get("failed_ids", 0) - original_failed
                    new_processed = self.stats.get("processed_ids", 0) - original_processed
                    
                    logger.info(f"Phase 2: Processed {new_processed} additional IDs, failed {len(failed_ids_second_phase)}")
                    
                    # Save failed IDs from second phase
                    await self.save_failed_ids(failed_ids_second_phase, "failed_ids_phase2.txt")
                else:
                    logger.info("No additional missing IDs found in Phase 2")
            else:
                logger.info("Skipping Phase 2 verification due to Elasticsearch unavailability")
                all_ids_second_phase = set()
                missing_ids_second_phase = set()
            
            # Save all failed IDs from both phases
            all_failed_ids = failed_ids_first_phase.union(failed_ids_second_phase)
            await self.save_failed_ids(all_failed_ids, "all_failed_ids.txt")
            
            # Update final stats
            self.stats["end_time"] = datetime.now()
            processing_time = (self.stats["end_time"] - self.stats["start_time"]).total_seconds()
            self.stats["processing_time_seconds"] = processing_time
            self.stats["total_ids_phase1"] = len(all_ids_first_phase)
            self.stats["total_ids_phase2"] = len(all_ids_second_phase)
            self.stats["missing_ids_phase1"] = len(missing_ids_first_phase)
            self.stats["missing_ids_phase2"] = len(missing_ids_second_phase)
            self.stats["failed_ids_phase1"] = len(failed_ids_first_phase)
            self.stats["failed_ids_phase2"] = len(failed_ids_second_phase)
            self.stats["total_failed_ids"] = len(all_failed_ids)
            self.stats["elasticsearch_available"] = elasticsearch_available
            
            # Log final stats
            logger.info("=== Processing Summary ===")
            logger.info(f"Processing completed in {processing_time:.2f} seconds")
            logger.info(f"Elasticsearch available: {elasticsearch_available}")
            logger.info(f"Phase 1 - Total IDs: {self.stats['total_ids_phase1']}")
            if elasticsearch_available:
                logger.info(f"Phase 1 - Indexed IDs: {len(indexed_ids_first_phase)}")
            logger.info(f"Phase 1 - Missing IDs: {self.stats['missing_ids_phase1']}")
            logger.info(f"Phase 1 - Failed IDs: {self.stats['failed_ids_phase1']}")
            if elasticsearch_available:
                logger.info(f"Phase 2 - Total IDs: {self.stats['total_ids_phase2']}")
                logger.info(f"Phase 2 - Indexed IDs: {len(indexed_ids_second_phase)}")
                logger.info(f"Phase 2 - Additional Missing IDs: {self.stats['missing_ids_phase2']}")
                logger.info(f"Phase 2 - Failed IDs: {self.stats['failed_ids_phase2']}")
            logger.info(f"Total Processed IDs: {self.stats.get('processed_ids', 0)}")
            logger.info(f"Total Failed IDs: {self.stats['total_failed_ids']}")
            
            return self.stats
        except Exception as e:
            logger.error(f"Error checking and processing missing pages: {str(e)}")
            self.stats["error"] = str(e)
            raise
        finally:
            # Close connections
            try:
                await self.elasticsearch_service.close()
            except Exception as e:
                logger.error(f"Error closing Elasticsearch connection: {str(e)}")
            
            try:
                await self.crawler.close()
            except Exception as e:
                logger.error(f"Error closing crawler connection: {str(e)}")


async def main():
    """Main entry point for the script."""
    # Configuration variables with hardcoded values
    output_dir = "/home/msc8/main_pages"
    elasticsearch_index = "edarehoquqy"
    num_parser_workers = 20
    num_embedding_workers = 5 
    num_indexing_workers = 20
    max_concurrent_requests = 5
    request_delay = 0.5
    
    try:
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
    except Exception as e:
        logger.error(f"Error in main function: {str(e)}")
        # Continue with a more detailed error message
        import traceback
        logger.error(f"Detailed error: {traceback.format_exc()}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
    except Exception as e:
        logger.error(f"Error running checker: {str(e)}")
        import traceback
        logger.error(f"Detailed error: {traceback.format_exc()}")



