import os
import asyncio
import logging
from typing import Dict, Any
import json
from aiopath import AsyncPath
import aiofiles

from src.processor import Processor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("app.log")
    ]
)

logger = logging.getLogger(__name__)


async def main(
    output_dir: str,
    num_parser_workers: int,
    num_embedding_workers: int,
    num_indexing_workers: int,
    parser_queue_size: int,
    embedding_queue_size: int,
    indexing_queue_size: int,
    elasticsearch_index: str,
    metadata_file: str = None,
) -> None:
    """
    Main entry point for the processor.
    
    Args:
        output_dir: Path to the output directory
        num_parser_workers: Number of parser workers
        num_embedding_workers: Number of embedding workers
        num_indexing_workers: Number of indexing workers
        parser_queue_size: Size of the parser queue
        embedding_queue_size: Size of the embedding queue
        indexing_queue_size: Size of the indexing queue
        elasticsearch_index: Name of the Elasticsearch index
        metadata_file: Path to a JSON file containing metadata
    """
    # Load metadata if provided
    metadata = {}
    if metadata_file:
        try:
            metadata_path = AsyncPath(metadata_file)
            if await metadata_path.exists():
                async with await aiofiles.open(metadata_file, "r") as f:
                    metadata_content = await f.read()
                    metadata = json.loads(metadata_content)
                logger.info(f"Loaded metadata from {metadata_file}")
            else:
                logger.warning(f"Metadata file {metadata_file} does not exist")
        except Exception as e:
            logger.error(f"Error loading metadata from {metadata_file}: {str(e)}")
    
    # Create processor
    processor = Processor(
        output_dir=output_dir,
        num_parser_workers=num_parser_workers,
        num_embedding_workers=num_embedding_workers,
        num_indexing_workers=num_indexing_workers,
        parser_queue_size=parser_queue_size,
        embedding_queue_size=embedding_queue_size,
        indexing_queue_size=indexing_queue_size,
        elasticsearch_index=elasticsearch_index,
        metadata=metadata,
    )
    
    # Process files
    stats = await processor.process_files()
    
    # Log information about failed IDs
    failed_ids_count = stats.get("failed_ids_count", 0)
    if failed_ids_count > 0:
        failed_ids_file = AsyncPath(output_dir) / "failed_ids.txt"
        logger.info(f"There were {failed_ids_count} failed IDs. See {failed_ids_file} for details.")
    else:
        logger.info("All files were processed successfully.")


if __name__ == "__main__":
    # Configuration variables
    output_dir = "../output"
    num_parser_workers = 5
    num_embedding_workers = 3
    num_indexing_workers = 5
    parser_queue_size = 100
    embedding_queue_size = 100
    indexing_queue_size = 100
    elasticsearch_index = "parsed_content"
    metadata_file = None  # Set to a path if needed
    
    try:
        asyncio.run(main(
            output_dir=output_dir,
            num_parser_workers=num_parser_workers,
            num_embedding_workers=num_embedding_workers,
            num_indexing_workers=num_indexing_workers,
            parser_queue_size=parser_queue_size,
            embedding_queue_size=embedding_queue_size,
            indexing_queue_size=indexing_queue_size,
            elasticsearch_index=elasticsearch_index,
            metadata_file=metadata_file,
        ))
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
    except Exception as e:
        logger.error(f"Error running processor: {str(e)}")
        raise