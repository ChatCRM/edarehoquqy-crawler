import os
import aiofiles
import logging
from pathlib import Path
from typing import List, Optional
from src.csv_export import EdarehoquqyDocument

logger = logging.getLogger(__name__)

async def document_to_html(doc: EdarehoquqyDocument, include_styles: bool = True) -> str:
    """
    Convert an EdarehoquqyDocument to HTML with Markdown-like styling.
    
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

async def save_html_file(doc: EdarehoquqyDocument, output_dir: Path) -> Path:
    """
    Save an EdarehoquqyDocument as an HTML file.
    
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
        file_path = output_dir / safe_filename
        
        # Generate HTML content
        html_content = await document_to_html(doc)
        
        # Save to file
        async with aiofiles.open(file_path, mode="w", encoding="utf-8") as file:
            await file.write(html_content)
            
        logger.info(f"Saved HTML file: {file_path}")
        return file_path
        
    except Exception as e:
        logger.error(f"Error saving HTML file for document {doc.id_edarehoquqy}: {e}")
        raise

async def export_documents_to_html(documents: List[EdarehoquqyDocument], output_dir: Optional[Path] = None) -> List[Path]:
    """
    Export multiple EdarehoquqyDocument objects to HTML files.
    
    Args:
        documents: List of documents to export
        output_dir: Directory to save files in (defaults to ~/edarehoquqy-html-output)
        
    Returns:
        List of paths to the saved files
    """
    if output_dir is None:
        output_dir = Path.home() / "edarehoquqy-html-output"
        
    saved_files = []
    for doc in documents:
        try:
            file_path = await save_html_file(doc, output_dir)
            saved_files.append(file_path)
        except Exception as e:
            logger.error(f"Failed to export document {doc.id_edarehoquqy}: {e}")
            
    logger.info(f"Exported {len(saved_files)} HTML files to {output_dir}")
    return saved_files 