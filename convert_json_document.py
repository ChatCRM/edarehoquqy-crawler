#!/usr/bin/env python3
import asyncio
import os
import json
import logging
from pathlib import Path

from src.csv_export import EdarehoquqyDocument
from src.html_export import save_html_file

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def main():
    # Define input and output paths
    input_file = Path("test_document.json") 
    output_dir = Path("test-output")
    os.makedirs(output_dir, exist_ok=True)
    
    # Get IranSans font path
    font_path = Path(__file__).parent / "assets" / "fonts" / "IRANSans.woff2"
    if not font_path.exists():
        logger.warning(f"IranSans font not found at {font_path}, HTML will use fallback fonts")
        font_path = None
    else:
        logger.info(f"Using IranSans font from {font_path}")
    
    try:
        # Load document from JSON file
        with open(input_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Create document object
        doc = EdarehoquqyDocument(
            id_edarehoquqy=data.get('id_edarehoquqy', ''),
            question=data.get('question', ''),
            answer=data.get('answer', ''),
            content=data.get('content', ''),
            title=data.get('title', ''),
            summary=data.get('summary', ''),
            metadata=data.get('metadata', {})  # Note: fixed key name to 'metadata' from 'metdata'
        )
        
        # Create output directory for the document
        doc_dir = output_dir / doc.id_edarehoquqy.replace('/', '_')
        os.makedirs(doc_dir, exist_ok=True)
        
        # Save document as HTML with the embedded font
        html_path = await save_html_file(doc, doc_dir, font_path=font_path)
        
        logger.info(f"Successfully converted document to HTML with IranSans font")
        logger.info(f"Output file saved to: {html_path}")
        
        return 0
    
    except Exception as e:
        logger.error(f"Error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 1

if __name__ == "__main__":
    asyncio.run(main()) 