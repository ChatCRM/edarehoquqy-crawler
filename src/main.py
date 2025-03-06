import os
import asyncio
import logging
import argparse
from typing import Dict, Any
import json
from pathlib import Path

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
            with open(metadata_file, "r") as f:
                metadata = json.load(f)
            logger.info(f"Loaded metadata from {metadata_file}")
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
    await processor.process_files()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process HTML files, extract content, generate embeddings, and index in Elasticsearch")
    
    parser.add_argument("--output-dir", type=str, default="../output", help="Path to the output directory")
    parser.add_argument("--num-parser-workers", type=int, default=5, help="Number of parser workers")
    parser.add_argument("--num-embedding-workers", type=int, default=3, help="Number of embedding workers")
    parser.add_argument("--num-indexing-workers", type=int, default=5, help="Number of indexing workers")
    parser.add_argument("--parser-queue-size", type=int, default=100, help="Size of the parser queue")
    parser.add_argument("--embedding-queue-size", type=int, default=100, help="Size of the embedding queue")
    parser.add_argument("--indexing-queue-size", type=int, default=100, help="Size of the indexing queue")
    parser.add_argument("--elasticsearch-index", type=str, default="parsed_content", help="Name of the Elasticsearch index")
    parser.add_argument("--metadata-file", type=str, help="Path to a JSON file containing metadata")
    
    args = parser.parse_args()
    
    try:
        asyncio.run(main(
            output_dir=args.output_dir,
            num_parser_workers=args.num_parser_workers,
            num_embedding_workers=args.num_embedding_workers,
            num_indexing_workers=args.num_indexing_workers,
            parser_queue_size=args.parser_queue_size,
            embedding_queue_size=args.embedding_queue_size,
            indexing_queue_size=args.indexing_queue_size,
            elasticsearch_index=args.elasticsearch_index,
            metadata_file=args.metadata_file,
        ))
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
    except Exception as e:
        logger.error(f"Error running processor: {str(e)}")
        raise