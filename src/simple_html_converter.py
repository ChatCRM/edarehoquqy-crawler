#!/usr/bin/env python3
"""
Simple HTML Converter for EdarehoquqyDocument

A standalone converter that doesn't require OpenAI or Elasticsearch.
"""

import argparse
import asyncio
import json
import logging
import os
import sys
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SimpleMetadata:
    """Simple metadata class to match the interface expected by the HTML generator."""
    
    def __init__(self, data):
        self.file_id = data.get('file_id', '')
        self.file_number = data.get('file_number', '')
        self.opinion_number = data.get('opinion_number', '')
        self.opinion_date = data.get('opinion_date', {})
        
class SimpleDocument:
    """Simple document class to match the interface expected by the HTML generator."""
    
    def __init__(self, data):
        self.id_edarehoquqy = data.get('id_edarehoquqy', '')
        self.question = data.get('question', '')
        self.answer = data.get('answer', '')
        self.summary = data.get('summary', '')
        self.title = data.get('title', '')
        
        # Create metadata object
        self.metdata = data.get('metdata', {})
        self.metadata = SimpleMetadata(self.metdata)
        
async def document_to_html(doc, include_styles=True):
    """
    Convert a document to HTML with Markdown-like styling.
    
    Args:
        doc: The document to convert
        include_styles: Whether to include CSS styles in the output
        
    Returns:
        HTML string representation of the document
    """
    # Basic styling for a clean, Markdown-like appearance
    styles = """
    <style>
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Open Sans', 'Helvetica Neue', sans-serif;
            line-height: 1.6;
            color: #333;
            max-width: 800px;
            margin: 0 auto;
            padding: 20px;
            direction: rtl;
            text-align: right;
        }
        h1, h2, h3 {
            border-bottom: 1px solid #eaecef;
            padding-bottom: 0.3em;
        }
        h1 {
            font-size: 2em;
            margin-top: 24px;
            margin-bottom: 16px;
        }
        h2 {
            font-size: 1.5em;
            margin-top: 24px;
            margin-bottom: 16px;
        }
        h3 {
            font-size: 1.25em;
            margin-top: 24px;
            margin-bottom: 16px;
        }
        pre {
            background-color: #f6f8fa;
            border-radius: 3px;
            padding: 16px;
            overflow: auto;
            font-family: 'SFMono-Regular', Consolas, 'Liberation Mono', Menlo, monospace;
        }
        blockquote {
            padding: 0 1em;
            color: #6a737d;
            border-left: 0.25em solid #dfe2e5;
            margin: 0 0 16px 0;
        }
        hr {
            height: 0.25em;
            padding: 0;
            margin: 24px 0;
            background-color: #e1e4e8;
            border: 0;
        }
        .metadata {
            background-color: #f6f8fa;
            padding: 16px;
            border-radius: 3px;
            margin-bottom: 20px;
        }
        .question {
            font-weight: bold;
            margin-top: 20px;
        }
        .answer {
            margin-bottom: 20px;
        }
        .summary {
            font-style: italic;
            background-color: #fffde7;
            padding: 16px;
            border-radius: 3px;
            margin: 20px 0;
        }
    </style>
    """
    
    # Generate metadata section with formatted dates
    metadata = doc.metadata
    
    # Handle date formatting
    shamsi_date = "نامشخص"
    gregorian_date = "نامشخص"
    
    if hasattr(metadata, 'opinion_date') and isinstance(metadata.opinion_date, dict):
        shamsi_date = metadata.opinion_date.get('shamsi', "نامشخص")
        gregorian_date = metadata.opinion_date.get('gregorian', "نامشخص")
    
    metadata_html = f"""
    <div class="metadata">
        <h3>اطلاعات سند</h3>
        <p><strong>شناسه سند:</strong> {doc.id_edarehoquqy}</p>
        <p><strong>شماره پرونده:</strong> {metadata.file_number}</p>
        <p><strong>شماره نظریه:</strong> {metadata.opinion_number}</p>
        <p><strong>تاریخ شمسی:</strong> {shamsi_date}</p>
        <p><strong>تاریخ میلادی:</strong> {gregorian_date}</p>
        <p><strong>شناسه فایل:</strong> {metadata.file_id}</p>
    </div>
    """
    
    # Generate content sections
    summary_html = f"""
    <div class="summary">
        <h3>خلاصه</h3>
        <p>{doc.summary}</p>
    </div>
    """ if doc.summary else ""
    
    question_answer_html = f"""
    <div class="question">
        <h2>سوال</h2>
        <p>{doc.question}</p>
    </div>
    <div class="answer">
        <h2>پاسخ</h2>
        <p>{doc.answer}</p>
    </div>
    """
    
    # Use the title field if available, otherwise use the opinion number
    title = doc.title if doc.title else f"نظریه شماره {metadata.opinion_number}"
    
    # Assemble the complete HTML document
    html_content = f"""<!DOCTYPE html>
<html lang="fa">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{title}</title>
    {styles if include_styles else ''}
</head>
<body>
    <h1>{title}</h1>
    {metadata_html}
    {summary_html}
    {question_answer_html}
    <hr>
    <p><em>تاریخ صدور: {shamsi_date}</em></p>
</body>
</html>"""
    
    return html_content

async def save_html_file(doc, output_dir):
    """
    Save a document as an HTML file.
    
    Args:
        doc: The document to save
        output_dir: Directory to save the file in
        
    Returns:
        Path to the saved file
    """
    try:
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Generate filename from document ID or opinion number
        filename = f"{doc.id_edarehoquqy}_{doc.metadata.opinion_number}.html"
        safe_filename = "".join(c if c.isalnum() or c in "._- " else "_" for c in filename)
        file_path = os.path.join(output_dir, safe_filename)
        
        # Generate HTML content
        html_content = await document_to_html(doc)
        
        # Save to file
        with open(file_path, mode="w", encoding="utf-8") as file:
            file.write(html_content)
            
        logger.info(f"Saved HTML file: {file_path}")
        return file_path
        
    except Exception as e:
        logger.error(f"Error saving HTML file for document {doc.id_edarehoquqy}: {e}")
        raise

async def convert_files(input_files, output_dir):
    """
    Convert JSON files to HTML documents.
    
    Args:
        input_files: List of input JSON file paths
        output_dir: Output directory for HTML files
        
    Returns:
        List of paths to saved HTML files
    """
    output_dir = Path(output_dir)
    os.makedirs(output_dir, exist_ok=True)
    
    saved_files = []
    
    for file_path in input_files:
        try:
            # Load JSON file
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Create document object
            doc = SimpleDocument(data)
            
            # Create subdirectory based on filename
            file_dir = output_dir / Path(file_path).stem
            os.makedirs(file_dir, exist_ok=True)
            
            # Save as HTML
            html_path = await save_html_file(doc, file_dir)
            saved_files.append(html_path)
            
            logger.info(f"Converted {file_path} to {html_path}")
        except Exception as e:
            logger.error(f"Error converting {file_path}: {e}")
            import traceback
            logger.error(traceback.format_exc())
    
    logger.info(f"Converted {len(saved_files)} files to HTML")
    return saved_files

async def main():
    parser = argparse.ArgumentParser(description="Convert JSON files to HTML without dependencies")
    parser.add_argument("files", nargs="+", help="JSON files to convert to HTML")
    parser.add_argument("-o", "--output-dir", help="Output directory (default: ./html-output)",
                       default="./html-output")
    
    args = parser.parse_args()
    
    output_dir = Path(args.output_dir)
    input_files = [Path(f) for f in args.files]
    
    try:
        saved_files = await convert_files(input_files, output_dir)
        logger.info(f"Successfully converted {len(saved_files)} files to HTML in {output_dir}")
    except Exception as e:
        logger.error(f"Error during conversion: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main()) 