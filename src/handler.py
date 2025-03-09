from httpx import RequestError, HTTPStatusError, AsyncClient
from bs4 import BeautifulSoup
import logging
import json
import aiofiles
from aiopath import AsyncPath
from typing import List, Dict, Any, Optional, Literal
import asyncio
import random
from pydantic import ValidationError
from src._core.schemas import (
    CustomSearchParams, SearchResponse, CrawlerConfig,
    IdeaPageInfo, ResultItem
)
from src._core import project_configs

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FileProcessor:
    """Handles file operations for the crawler"""
    def __init__(self, base_path: AsyncPath):
        self.base_path = base_path
        self.main_pages_path = self.base_path / "main_pages"
        self.pages_path = self.base_path / "pages"
        self.links_file = self.base_path / "links.txt"
        self.failed_file = self.base_path / "failed.txt"
        # Store IDs in memory instead of writing to files for each ID
        self.successful_ids = set()
        self.failed_ids = {}  # Store idea_id -> error_message
    
    async def initialize(self) -> None:
        """Create necessary directories"""
        logger.info(f"Initializing directories at {self.base_path}")
        await self.base_path.mkdir(parents=True, exist_ok=True)
        await self.main_pages_path.mkdir(parents=True, exist_ok=True)
        await self.pages_path.mkdir(parents=True, exist_ok=True)
        logger.info("Directories initialized successfully")
    
    def is_valid_html(self, html_content: str) -> bool:
        """
        Validate HTML content to ensure it doesn't contain error messages
        
        Args:
            html_content: HTML content to validate
            
        Returns:
            bool: True if the HTML is valid, False if it contains error messages
        """
        # More efficient to check once rather than looping through each error text
        if "خطایی رخ داده است" in html_content:
            return False
        return True
    
    async def save_main_page(self, page_number: int, html_content: str) -> bool:
        """
        Save main page HTML if it's valid
        
        Args:
            page_number: Page number
            html_content: HTML content to save
            
        Returns:
            bool: True if saved successfully, False otherwise
        """
            
        file_path = self.main_pages_path / f"page_{page_number}.html"
        async with aiofiles.open(file_path, "w", encoding="utf-8") as f:
            await f.write(html_content)
            # Reduce logging
            # logger.info(f"Saved main page {page_number} HTML")
        return True

    async def save_idea_page(self, idea_id: str, html_content: str) -> bool:
        """
        Save individual idea page HTML if it's valid
        
        Args:
            idea_id: Idea ID
            html_content: HTML content to save
            
        Returns:
            bool: True if saved successfully, False otherwise
        """
        if not self.is_valid_html(html_content):
            return False
            
        idea_dir = self.pages_path / str(idea_id)
        await AsyncPath(idea_dir).mkdir(parents=True, exist_ok=True)
        file_path = idea_dir / f"{idea_id}.html"
        async with aiofiles.open(file_path, "w", encoding="utf-8") as f:
            await f.write(html_content)
            # Reduce logging
            # logger.info(f"Saved idea page {idea_id} HTML")
        return True

    async def save_idea_metadata(self, idea_id: str, metadata: Dict[str, Any]) -> None:
        """Save idea metadata as JSON"""
        idea_dir = self.pages_path / str(idea_id)
        await AsyncPath(idea_dir).mkdir(parents=True, exist_ok=True)
        file_path = idea_dir / f"{idea_id}_metadata.json"
        async with aiofiles.open(file_path, "w", encoding="utf-8") as f:
            await f.write(json.dumps(metadata, ensure_ascii=False, indent=2))
            
    async def get_saved_file_path(self, page_number: int, params: Dict[str, Any]) -> Optional[AsyncPath]:
        """Get the path to a saved search results file"""
        search_text = params.get("search", "")
        from_date = params.get("fromDate", "")
        to_date = params.get("toDate", "")
        
        search_term_part = f"_{search_text}" if search_text else ""
        date_part = f"_{from_date}_to_{to_date}" if from_date or to_date else ""
        filename = f"legal_opinions{search_term_part}{date_part}_page{page_number}.json"
        
        file_path = self.base_path / filename
        if await file_path.exists():
            return file_path
        return None
        
    async def load_saved_search_response(self, file_path: AsyncPath) -> Optional[SearchResponse]:
        """Load a saved search response from a file"""
        try:
            async with aiofiles.open(file_path, "r", encoding="utf-8") as f:
                data = json.loads(await f.read())
                return SearchResponse(
                    results=[ResultItem(**item) for item in data.get("results", [])],
                    totalHits=data.get("totalHits", 0),
                    more=data.get("more", False)
                )
        except Exception as e:
            logger.error(f"Failed to load saved search response: {e}")
            return None

    def add_successful_id(self, idea_id: str) -> None:
        """Add successful ID to memory set"""
        self.successful_ids.add(idea_id)
    
    def add_failed_id(self, idea_id: str, error: str) -> None:
        """Add failed ID to memory dict"""
        self.failed_ids[idea_id] = error
        logger.error(f"Failed to process idea {idea_id}: {error}")

    async def save_all_ids(self) -> None:
        """Save all successful and failed IDs to their respective files"""
        # Save successful IDs
        if self.successful_ids:
            async with aiofiles.open(self.links_file, "w", encoding="utf-8") as f:
                await f.write("\n".join(sorted(self.successful_ids)) + "\n")
            logger.info(f"Saved {len(self.successful_ids)} successful IDs to {self.links_file}")
        
        # Save failed IDs
        if self.failed_ids:
            async with aiofiles.open(self.failed_file, "w", encoding="utf-8") as f:
                for idea_id, error in sorted(self.failed_ids.items()):
                    await f.write(f"{idea_id}: {error}\n")
            logger.info(f"Saved {len(self.failed_ids)} failed IDs to {self.failed_file}")

    async def summarize_results(self) -> Dict[str, int]:
        """Summarize the results of the crawling process"""
        # Save all IDs to files before summarizing
        await self.save_all_ids()
        
        successful_count = len(self.successful_ids)
        failed_count = len(self.failed_ids)
        total_count = successful_count + failed_count
        
        logger.info(f"Crawling summary: {successful_count} successful, {failed_count} failed, {total_count} total")
        return {
            "successful": successful_count,
            "failed": failed_count,
            "total": total_count
        }

class AsyncCrawlerClient:
    def __init__(self, config: CrawlerConfig):
        self.config = config
        self.client = AsyncClient(follow_redirects=True, timeout=config.timeout)
        self.verification_token = None
        self._base_url: str = None
        self._headers = {}
        self._cookies = {}

    @property
    def cookies(self):
        return self._cookies

    @cookies.setter
    def cookies(self, cookie: Dict[str, str]) -> None:
        self._cookies.update(cookie)

    @property
    def headers(self):
        self._headers.update({ 
            "accept": "*/*",
            "accept-language": "en-US,en;q=0.9",
            "moduleid": "1286",
            "priority": "u=1, i",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
            "x-requested-with": "XMLHttpRequest",
            "sec-ch-ua": '"Not(A:Brand";v="99", "Google Chrome";v="133", "Chromium";v="133"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "tabid": "495",
        })
        return self._headers

    @headers.setter
    def headers(self, headers: Dict[str, str]) -> None:
        self._headers.update(headers)

    @property
    def BASE_URL(self) -> str:
        return self._base_url

    @BASE_URL.setter
    def BASE_URL(self, mode: Literal["nazarat_mashverati", "normal_search"]) -> None:
        # Just set the base URL, don't include the path
        self._base_url = "https://edarehoquqy.eadl.ir/%D8%AC%D8%B3%D8%AA%D8%AC%D9%88%DB%8C-%D9%86%D8%B8%D8%B1%DB%8C%D8%A7%D8%AA-%D9%85%D8%B4%D9%88%D8%B1%D8%AA%DB%8C/%D8%AC%D8%B3%D8%AA%D8%AC%D9%88%DB%8C-%D9%86%D8%B8%D8%B1%DB%8C%D9%87/%D8%AC%D8%B3%D8%AA%D8%AC%D9%88%DB%8C-%D9%86%D8%B8%D8%B1%DB%8C%D8%A7%D8%AA-%D9%85%D8%B4%D9%88%D8%B1%D8%AA%DB%8C/%D8%AC%D8%B3%D8%AA%D8%AC%D9%88%DB%8C-%D9%86%D8%B8%D8%B1%DB%8C%D9%87"

    @property
    def SEARCH_PAGE_URL(self) -> str:
        return f"{self.BASE_URL}/%D8%AC%D8%B3%D8%AA%D8%AC%D9%88%DB%8C-%D9%86%D8%B8%D8%B1%DB%8C%D8%A7%D8%AA-%D9%85%D8%B4%D9%88%D8%B1%D8%AA%DB%8C/%D8%AC%D8%B3%D8%AA%D8%AC%D9%88%DB%8C-%D9%86%D8%B8%D8%B1%DB%8C%D9%87"

    @property
    def SEARCH_API_URL(self) -> str:
        return f"https://edarehoquqy.eadl.ir/API/Mvc/IdeaProject.IdeaSearch/CustomSearch/Search"

    @property
    def IDEA_DETAIL_URL(self) -> str:
        return f"https://edarehoquqy.eadl.ir/%D8%AC%D8%B3%D8%AA%D8%AC%D9%88%DB%8C-%D9%86%D8%B8%D8%B1%DB%8C%D8%A7%D8%AA-%D9%85%D8%B4%D9%88%D8%B1%D8%AA%DB%8C/%D8%AC%D8%B3%D8%AA%D8%AC%D9%88%DB%8C-%D9%86%D8%B8%D8%B1%DB%8C%D9%87/moduleId/1286/controller/Search/action/Detail"

    async def initialize_session(self) -> bool:
        try:
            logger.info(f"Initializing session with URL: {self.SEARCH_PAGE_URL}")
            response = await self.client.get(self.SEARCH_PAGE_URL)
            response.raise_for_status()

            # Parse the page to get the verification token
            soup = BeautifulSoup(response.text, "html.parser")
            token_input = soup.select_one('input[name="__RequestVerificationToken"]')

            if not token_input:
                logger.error("Verification token not found in the page")
                return False

            self.verification_token = token_input.get("value")
            self.cookies = {k: v for k, v in self.client.cookies.items()}
            logger.debug(f"Cookies set: {self.cookies}")

            self.headers = {
                "requestverificationtoken": self.verification_token,
                "referer": self.SEARCH_PAGE_URL,
            }
            logger.debug(f"Headers set: {self.headers}")

            logger.info(f"Session initialized successfully with token: {self.verification_token[:10]}...")
            return True

        except Exception as e:
            logger.error(f"Failed to initialize session: {str(e)}")
            return False

    async def get_idea_page(self, idea_id: str) -> Optional[str]:
        """Fetch the HTML content for an idea page"""
        try:
            url = f"{self.IDEA_DETAIL_URL}?IdeaId={idea_id}"
            await asyncio.sleep(1)
            response = await self.client.get(url, headers=self.headers, cookies=self.cookies)
            response.raise_for_status()
            return response.text
        except Exception as e:
            logger.error(f"Failed to fetch idea page {idea_id}: {e}")
            return None

    async def close(self) -> None:
        await self.client.aclose()

class LegalOpinionsCrawler(AsyncCrawlerClient):
    """Crawler for legal opinions with queue-based processing"""
    def __init__(self, config: CrawlerConfig, file_processor: Optional[FileProcessor] = None):
        super().__init__(config)
        self.config = config
        self.queue = asyncio.Queue(maxsize=config.max_queue_size)
        self.processing = False
        self._workers: List[asyncio.Task] = []
        self.stop_event = asyncio.Event()
        
        # Create or use file processor
        self.file_processor = file_processor or FileProcessor(config.base_path)
        
        # Create output directories for backward compatibility
        self.main_pages_path = config.base_path / "main_pages"
        self.pages_path = config.base_path / "pages"
        self.main_pages_path.mkdir(parents=True, exist_ok=True)
        self.pages_path.mkdir(parents=True, exist_ok=True)

    async def process_main_page(self, page_number: int, response: SearchResponse) -> List[IdeaPageInfo]:
        """Process a main search results page"""
        try:
            # Save the main page HTML if available
            if response.raw_html:
                save_result = await self.save_main_page(page_number, response.raw_html)
                if not save_result:
                    logger.warning(f"Main page {page_number} HTML contains error message, but continuing to process results")

            # Extract and return idea page info
            idea_pages = []
            for result in response.results:
                # Use DocumentUrl as idea_id if IdeaId is not available
                idea_id = str(result.IdeaId) if result.IdeaId is not None else result.DocumentUrl
                
                idea_pages.append(IdeaPageInfo(
                    idea_id=idea_id,
                    document_url=result.DocumentUrl,
                    page_number=page_number,
                    raw_data=result.model_dump()
                ))
            return idea_pages
        except Exception as e:
            logger.error(f"Error processing main page {page_number}: {e}")
            return []

    async def process_idea_page(self, idea_info: IdeaPageInfo) -> None:
        """Process an individual idea page"""
        try:
            # Save metadata first
            await self.save_idea_metadata(idea_info.idea_id, idea_info.raw_data)

            # Fetch and save the HTML
            await asyncio.sleep(random.uniform(0.5, 1.5))
            html_content = await self.get_idea_page(idea_info.idea_id)
            if html_content:
                # Try to save the HTML content, checking for error messages
                if await self.save_idea_page(idea_info.idea_id, html_content):
                    # Add successful ID to memory set only if save was successful
                    self.file_processor.add_successful_id(idea_info.idea_id)
                else:
                    error_msg = f"HTML content for idea {idea_info.idea_id} contains error message"
                    # Add failed ID to memory dict
                    self.file_processor.add_failed_id(idea_info.idea_id, error_msg)
            else:
                error_msg = f"Failed to fetch HTML for idea {idea_info.idea_id}"
                # Add failed ID to memory dict
                self.file_processor.add_failed_id(idea_info.idea_id, error_msg)
        except Exception as e:
            error_msg = f"Error processing idea {idea_info.idea_id}: {e}"
            # Add failed ID to memory dict
            self.file_processor.add_failed_id(idea_info.idea_id, str(e))

    async def worker(self) -> None:
        """Worker to process items from the queue"""
        while self.processing and not self.stop_event.is_set():
            idea_info = None
            try:
                idea_info = await self.queue.get()
                await self.process_idea_page(idea_info)
                await asyncio.sleep(self.config.request_delay)
            except asyncio.CancelledError:
                if idea_info:
                    # Add as failed if the task was cancelled
                    self.file_processor.add_failed_id(idea_info.idea_id, "Task cancelled")
                break
            except Exception as e:
                error_msg = f"Worker error: {e}"
                logger.error(error_msg)
                if idea_info:
                    # Add as failed if there was an error
                    self.file_processor.add_failed_id(idea_info.idea_id, error_msg)
            finally:
                if idea_info:
                    # Always mark the task as done
                    self.queue.task_done()

    async def start_workers(self) -> None:
        """Start worker tasks"""
        self.processing = True
        self.stop_event.clear()
        self._workers = [
            asyncio.create_task(self.worker())
            for _ in range(self.config.max_concurrent_requests)
        ]

    async def stop_workers(self) -> None:
        """Stop worker tasks"""
        self.processing = False
        self.stop_event.set()
        for worker in self._workers:
            worker.cancel()
        await asyncio.gather(*self._workers, return_exceptions=True)
        self._workers.clear()

    async def retry_failed_ids(self) -> None:
        """
        Retry processing failed IDs at the end of the crawl
        """
        if not self.file_processor.failed_ids:
            logger.info("No failed IDs to retry")
            return
            
        logger.info(f"Retrying {len(self.file_processor.failed_ids)} failed IDs")
        
        # Create a copy of failed IDs to iterate over
        failed_ids = list(self.file_processor.failed_ids.keys())
        self.file_processor.failed_ids.clear()
        retried = 0
        
        for idea_id in failed_ids:
            # Get the raw data if available
            raw_data = None
            idea_dir = self.file_processor.pages_path / str(idea_id)
            metadata_file = idea_dir / f"{idea_id}_metadata.json"
            
            try:
                # Check if metadata exists
                if await AsyncPath(metadata_file).exists():
                    async with aiofiles.open(metadata_file, "r", encoding="utf-8") as f:
                        raw_data = json.loads(await f.read())
                
                # Create idea info object
                idea_info = IdeaPageInfo(
                    idea_id=idea_id,
                    document_url=raw_data.get("DocumentUrl", "") if raw_data else "",
                    page_number=0,  # Not important for retry
                    raw_data=raw_data or {}
                )
                
                # Process through the main flow
                await self.process_idea_page(idea_info)
                retried += 1
                
                # Add a small delay between retries
                await asyncio.sleep(self.config.request_delay)
                
            except Exception as e:
                logger.error(f"Error retrying idea {idea_id}: {e}")
        
        logger.info(f"Completed retrying {retried} failed IDs")

    async def crawl_all(self, start_page: int = 1, end_page: Optional[int] = None) -> None:
        """Crawl all pages and their ideas"""
        try:
            # Initialize file processor
            logger.info("Initializing file processor")
            await self.file_processor.initialize()
            
            logger.info("Starting worker tasks")
            await self.start_workers()

            current_page = start_page
            logger.info(f"Starting crawl from page {current_page} to {end_page or 'end'}")
            
            while end_page is None or current_page <= end_page:
                params = CustomSearchParams(
                    search="",
                    pageIndex=current_page,
                    pageSize=10,
                    sortOption=1
                )

                response = await self.search(params)
                if not response:
                    logger.error(f"Failed to fetch page {current_page}")
                    break

                # Process main page and queue idea pages
                idea_pages = await self.process_main_page(current_page, response)
                
                # Queue idea pages without logging each one
                for idea_info in idea_pages:
                    await self.queue.put(idea_info)

                if not response.more:
                    break

                current_page += 1
                await asyncio.sleep(self.config.request_delay)

            # Wait for all tasks to complete
            logger.info("Waiting for all queued tasks to complete")
            await self.queue.join()
            logger.info("All tasks completed successfully")
            
            # Retry failed IDs
            await self.retry_failed_ids()
            
            # Summarize the results
            logger.info("Generating crawling summary")
            summary = await self.file_processor.summarize_results()
            logger.info(f"Crawling completed: {summary['successful']} successful, {summary['failed']} failed, {summary['total']} total")
        finally:
            logger.info("Stopping worker tasks")
            await self.stop_workers()

    async def search(self, params: CustomSearchParams) -> Optional[SearchResponse]:
        """
        Perform a search using the given parameters

        Args:
            params: CustomSearchParams with search parameters

        Returns:
            Optional[SearchResponse]: The search response or None if the request failed
        """
        if not self.verification_token:
            if not await self.initialize_session():
                return None

        try:
            # Make the API request
            response = await self.client.get(
                self.SEARCH_API_URL,
                params=params.model_dump(),
                headers=self.headers,
                cookies=self.cookies,
            )
            response.raise_for_status()

            # Parse the response
            response_data = response.json()
            
            try:
                # Store the raw HTML response
                raw_html = response.text
                
                # Create the search response
                search_response = SearchResponse(
                    results=[ResultItem(**item) for item in response_data.get("results", [])],
                    totalHits=response_data.get("totalHits", 0),
                    more=response_data.get("more", False),
                    raw_html=raw_html
                )
                
                # Save the raw HTML for the main page
                page_number = params.pageIndex
                save_result = await self.save_main_page(page_number, raw_html)
                
                return search_response
            except ValidationError as e:
                logger.error(f"Failed to parse search response: {e}")
                # Log the actual response data for debugging
                logger.error(f"Response data: {response_data}")
                return None

        except Exception as e:
            logger.error(f"An error occurred during search: {str(e)}")
            return None

    async def find_last_saved_page(
        self,
        search_text: str = "",
        page_size: int = 10,
        sort_option: int = 1,
        from_date: str = "",
        to_date: str = ""
    ) -> int:
        """
        Find the last saved page number for the given search parameters

        Args:
            search_text: Text to search for
            page_size: Number of results per page
            sort_option: Sort option
            from_date: Start date for filtering
            to_date: End date for filtering

        Returns:
            int: The last saved page number, or 0 if no pages are saved
        """
        if not project_configs.OUTPUT_PATH:
            return 0

        # Create the pattern for matching filenames
        search_term_part = f"_{search_text}" if search_text else ""
        date_part = f"_{from_date}_to_{to_date}" if from_date or to_date else ""
        pattern = f"legal_opinions{search_term_part}{date_part}_page"

        # Find all matching files
        last_page = 0
        output_dir = AsyncPath(project_configs.OUTPUT_PATH)

        if not await output_dir.exists():
            return 0

        for file_path in await output_dir.glob(f"{pattern}*.json"):
            try:
                # Extract page number from filename
                filename = file_path.name
                page_str = filename.split('_page')[1].split('.json')[0]
                page_num = int(page_str)
                last_page = max(last_page, page_num)
            except (ValueError, IndexError):
                continue

        return last_page

    async def crawl_all_results(
        self,
        search_text: str = "",
        page_size: int = 10,
        sort_option: int = 1,
        from_date: str = "",
        to_date: str = "",
    ) -> List[Dict[str, Any]]:
        """
        Crawl all search results with pagination, skipping already saved pages

        Args:
            search_text: Text to search for
            page_size: Number of results per page (10, 20, or 30)
            sort_option: Sort option (0 or 1)
            from_date: Start date for filtering
            to_date: End date for filtering

        Returns:
            List of all search results
        """
        try:
            # Initialize file processor
            await self.file_processor.initialize()
            
            # Start workers
            await self.start_workers()
            
            all_results = []
            current_page = 1
            has_more = True
            last_saved_page = await self.find_last_saved_page(search_text, page_size, sort_option, from_date, to_date)

            if last_saved_page > 0:
                logger.info(f"Found previously saved results up to page {last_saved_page}")
                current_page = last_saved_page

                # Load results from the last saved page to check if there are more pages
                params = CustomSearchParams(
                    search=search_text,
                    pageIndex=current_page,
                    pageSize=page_size,
                    sortOption=sort_option,
                    fromDate=from_date,
                    toDate=to_date,
                )

                file_path = await self.file_processor.get_saved_file_path(current_page, params.model_dump())
                if file_path and await file_path.exists():
                    search_response = await self.file_processor.load_saved_search_response(file_path)
                    if search_response:
                        all_results.extend(search_response.results)
                        has_more = search_response.more
                        if has_more:
                            current_page += 1
            
            # Initialize the session before starting new requests
            if has_more and not self.verification_token:
                if not await self.initialize_session():
                    logger.error("Failed to initialize session, aborting crawl")
                    return all_results

            logger.info(f"Starting crawl from page {current_page}")
            
            while has_more:
                params = CustomSearchParams(
                    search=search_text,
                    pageIndex=current_page,
                    pageSize=page_size,
                    sortOption=sort_option,
                    fromDate=from_date,
                    toDate=to_date,
                )

                search_response = await self.search(params)

                if not search_response:
                    logger.error(f"Failed to get results for page {current_page}, stopping pagination")
                    break

                # Process the main page
                idea_pages = await self.process_main_page(current_page, search_response)
                
                # Queue the idea pages for processing
                for idea_info in idea_pages:
                    await self.queue.put(idea_info)

                # Add the results from this page
                all_results.extend(search_response.results)

                # Check if there are more pages
                has_more = search_response.more

                if has_more:
                    current_page += 1
                    await asyncio.sleep(self.config.request_delay)

            # Wait for all tasks to complete
            logger.info("Waiting for all queued tasks to complete")
            await self.queue.join()
            
            # Retry failed IDs
            await self.retry_failed_ids()
            
            # Summarize the results
            logger.info("Generating crawling summary")
            summary = await self.file_processor.summarize_results()
            logger.info(f"Crawling completed: {summary['successful']} successful, {summary['failed']} failed, {summary['total']} total")
            
            logger.info(f"Collected {len(all_results)} total results")
            return all_results
        finally:
            # Stop workers
            await self.stop_workers()

    async def save_main_page(self, page_number: int, html_content: str) -> bool:
        """
        Save main page HTML
        
        Returns:
            bool: True if saved successfully, False if HTML contains error message
        """
        return await self.file_processor.save_main_page(page_number, html_content)

    async def save_idea_page(self, idea_id: str, html_content: str) -> bool:
        """
        Save individual idea page HTML
        
        Returns:
            bool: True if saved successfully, False if HTML contains error message
        """
        return await self.file_processor.save_idea_page(idea_id, html_content)

    async def save_idea_metadata(self, idea_id: str, metadata: Dict[str, Any]) -> None:
        """Save idea metadata as JSON"""
        await self.file_processor.save_idea_metadata(idea_id, metadata)
