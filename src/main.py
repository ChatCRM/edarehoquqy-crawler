import asyncio
import logging
from pathlib import Path
from src.handler import LegalOpinionsCrawler, FileProcessor
from src._core.schemas import CrawlerConfig
import time

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

async def main():
    try:
        # Initialize the crawler with configuration
        config = CrawlerConfig(
            max_concurrent_requests=10,
            max_queue_size=500,
            request_delay=1,
            timeout=30,
            base_path=Path("output")
        )
        
        logging.info("Initializing crawler")
        # Create file processor
        file_processor = FileProcessor(config.base_path)
        
        # Create crawler with file processor
        crawler = LegalOpinionsCrawler(config, file_processor)
        
        # Set base URL and initialize session
        crawler.BASE_URL = "nazarat_mashverati"
        
        if not await crawler.initialize_session():
            logging.error("Failed to initialize crawler session")
            return

        # Start crawling
        logging.info("Starting crawler")
        await crawler.crawl_all(start_page=1, end_page=10)  # Crawl 5 pages for testing

    except Exception as e:
        logging.error(f"An error occurred: {e}")
    finally:
        if 'crawler' in locals():
            logging.info("Closing crawler client")
            await crawler.close()
        logging.info("Crawler execution completed")

if __name__ == "__main__":
    start_time = time.time()
    asyncio.run(main())
    end_time = time.time()
    print(f"Time taken: {end_time - start_time:.2f} seconds")