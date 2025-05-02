#!/usr/bin/env python3
"""
Word Exporter Script

This script processes JSON files from a directory structure and exports them to Word documents.
The directory structure is expected to be:
search/
  keyword1/
    id1/
      file.json
    id2/
      file.json
  keyword2/
    ...

For each keyword, a combined Word document will be created containing all documents.
Additionally, documents without a category will be placed in a 'بدون-دسته-بندی' directory.
"""

import asyncio
import argparse
import logging
from pathlib import Path
from src.word_export import FileLoader, WordExporter

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def run_export(search_dir: str, output_dir: str = None):
    """Run the export process with the given directory"""
    try:
        # If output_dir is provided, use that instead of the default
        search_path = search_dir
        if output_dir:
            # Make output directory if it doesn't exist
            Path(output_dir).mkdir(parents=True, exist_ok=True)
            
        # Initialize the file loader and exporter
        loader = FileLoader(search_path)
        exporter = WordExporter(loader, output_dir)
        
        # Export all keywords to Word documents
        logger.info(f"Starting export from {search_path}")
        results = await exporter.export_all_keywords()
        
        # Log results
        if results:
            logger.info(f"Successfully exported {len(results)} keywords to Word documents:")
            for keyword, path in results.items():
                logger.info(f"  - {keyword}: {path}")
        else:
            logger.warning("No documents were exported")
            
        # Now process uncategorized documents
        logger.info("Now processing uncategorized documents...")
        try:
            uncategorized_path = await exporter.export_uncategorized_documents()
            if uncategorized_path:
                logger.info(f"Successfully exported uncategorized documents to: {uncategorized_path}")
            else:
                logger.info("No uncategorized documents found")
        except Exception as e:
            logger.error(f"Error processing uncategorized documents: {e}")
            
        # Log failures
        failed_ids = loader.get_failed_ids()
        if failed_ids:
            logger.warning(f"Failed to load {len(failed_ids)} document IDs")
            
        failed_files = loader.get_failed_files()
        if failed_files:
            logger.warning(f"Failed to load {len(failed_files)} files")
            
    except Exception as e:
        logger.error(f"Error during export: {e}")
        raise

def main():
    """Parse command line arguments and run the export"""
    parser = argparse.ArgumentParser(description="Export JSON documents to Word files")
    parser.add_argument(
        "--search-dir", 
        type=str, 
        default="search",
        help="Directory containing keyword subdirectories with JSON files"
    )
    parser.add_argument(
        "--output-dir", 
        type=str, 
        help="Optional output directory (default is to save in keyword directories)"
    )
    
    args = parser.parse_args()
    
    # Run the export
    asyncio.run(run_export(args.search_dir, args.output_dir))
    
if __name__ == "__main__":
    main() 