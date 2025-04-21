#!/usr/bin/env python3
"""
Example script to convert and save documents from Elasticsearch to HTML files.

This script demonstrates how to:
1. Connect to Elasticsearch
2. Search for documents
3. Convert them to HTML
4. Save them to disk

Usage:
    python convert_documents.py --query "سرقت" --output-dir "./output" --generate-titles
"""

import asyncio
import os
import sys
import argparse
from pathlib import Path
import logging
from dotenv import load_dotenv
from openai import AsyncOpenAI

# Add parent directory to path to import from src
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.html_converter import convert_and_save_documents

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def main():
    # Load environment variables
    load_dotenv()
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Convert Elasticsearch documents to HTML")
    parser.add_argument("--query", "-q", required=True, nargs="+",
                        help="Search queries to retrieve documents from Elasticsearch")
    parser.add_argument("--output-dir", "-o", default="./html-output",
                        help="Output directory for HTML files")
    parser.add_argument("--generate-titles", "-t", action="store_true",
                        help="Generate titles and summaries for documents")
    
    args = parser.parse_args()
    
    # Create output directory if it doesn't exist
    output_dir = Path(args.output_dir)
    os.makedirs(output_dir, exist_ok=True)
    
    # Initialize OpenAI client if generating titles/summaries
    client = None
    if args.generate_titles:
        logger.info("Initializing OpenAI client")
        client = AsyncOpenAI(
            base_url=os.getenv("OPENAI_BASE_URL"),
            api_key=os.getenv("OPENAI_API_KEY")
        )
    
    try:
        # Convert and save documents
        logger.info(f"Converting documents for queries: {args.query}")
        html_files = await convert_and_save_documents(args.query, output_dir, client)
        
        # Print results
        logger.info(f"Converted {len(html_files)} documents to HTML")
        logger.info(f"Files saved to: {output_dir}")
        
        # Print list of generated files
        if html_files:
            logger.info("Generated files:")
            for file_path in html_files:
                logger.info(f"  - {file_path}")
    
    except Exception as e:
        logger.error(f"Error converting documents: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(asyncio.run(main())) 