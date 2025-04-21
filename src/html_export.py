import os
import aiofiles
import logging
import base64
import json
from pathlib import Path
from typing import List, Optional
from src.csv_export import EdarehoquqyDocument

logger = logging.getLogger(__name__)

async def get_font_as_base64(font_path: Path) -> str:
    """
    Read a font file and convert it to base64 encoding.
    
    Args:
        font_path: Path to the font file
        
    Returns:
        Base64 encoded string of the font file
    """
    try:
        async with aiofiles.open(font_path, mode="rb") as font_file:
            font_data = await font_file.read()
            return base64.b64encode(font_data).decode("utf-8")
    except Exception as e:
        logger.error(f"Error reading font file {font_path}: {e}")
        return ""

async def document_to_html(doc: EdarehoquqyDocument, include_styles: bool = True, font_path: Optional[Path] = None) -> str:
    """
    Convert an EdarehoquqyDocument to HTML with Markdown-like styling.
    
    Args:
        doc: The document to convert
        include_styles: Whether to include CSS styles in the output
        font_path: Optional path to the IranSans font file
        
    Returns:
        HTML string representation of the document
    """
    # Font embedding
    font_face = ""
    if font_path and font_path.exists():
        try:
            font_base64 = await get_font_as_base64(font_path)
            if font_base64:
                font_ext = font_path.suffix.lower().lstrip('.')
                mime_type = f"font/{font_ext}" if font_ext != "ttf" else "font/truetype"
                font_face = f"""
                @font-face {{
                    font-family: 'IranSans';
                    src: url('data:{mime_type};base64,{font_base64}') format('{font_ext}');
                    font-weight: normal;
                    font-style: normal;
                    font-display: swap;
                }}
                """
        except Exception as e:
            logger.error(f"Error embedding font: {e}")
    
    # If no font file was provided or there was an error loading it, use a fallback
    if not font_face:
        font_face = """
        @font-face {
            font-family: 'IranSans';
            src: local('IranSans'), local('Iran Sans'), local('Vazir'), local('Tahoma');
            font-weight: normal;
            font-style: normal;
        }
        """
    
    # Basic styling for a clean, Markdown-like appearance
    styles = f"""
    <style>
        {font_face}
        body {{
            font-family: 'IranSans', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Open Sans', 'Helvetica Neue', sans-serif;
            line-height: 1.8;
            color: #333;
            max-width: 800px;
            margin: 0 auto;
            padding: 2rem;
            direction: rtl;
            text-align: right;
            background-color: #fcfcfc;
        }}
        h1, h2, h3 {{
            font-family: 'IranSans', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Open Sans', 'Helvetica Neue', sans-serif;
            border-bottom: 2px solid #e0e0e0;
            padding-bottom: 0.5em;
            color: #222;
            margin-top: 1.5rem;
            margin-bottom: 1rem;
        }}
        h1 {{
            font-size: 2em;
            text-align: center;
        }}
        h2 {{
            font-size: 1.5em;
            color: #2c3e50;
        }}
        h3 {{
            font-size: 1.25em;
            color: #34495e;
        }}
        pre {{
            background-color: #f8f9fa;
            border-radius: 4px;
            padding: 1rem;
            overflow: auto;
            font-family: 'IranSans', 'SFMono-Regular', Consolas, 'Liberation Mono', Menlo, monospace;
            border: 1px solid #e9ecef;
        }}
        blockquote {{
            padding: 0.5rem 1rem;
            color: #495057;
            border-right: 0.25rem solid #6c757d;
            border-left: none;
            margin: 0 0 1rem 0;
            background-color: #f8f9fa;
        }}
        hr {{
            height: 1px;
            margin: 2rem 0;
            background-color: #e9ecef;
            border: 0;
        }}
        .metadata {{
            background-color: #f8f9fa;
            padding: 1.5rem;
            border-radius: 4px;
            margin-bottom: 2rem;
            border: 1px solid #dee2e6;
            box-shadow: 0 1px 3px rgba(0, 0, 0, 0.05);
            font-size: 1em;
        }}
        .metadata h3 {{
            font-size: 2em;
            color: #222;
        }}
        .metadata p {{
            margin: 0.5rem 0;
        }}
        .question {{
            font-weight: 600;
            margin-top: 2rem;
            background-color: #f1f8ff;
            padding: 1rem;
            border-right: 4px solid #4a6fa5;
            border-radius: 4px;
        }}
        .answer {{
            margin-bottom: 2rem;
            padding: 1rem;
            background-color: #fff;
            border: 1px solid #e9ecef;
            border-radius: 4px;
            box-shadow: 0 1px 2px rgba(0, 0, 0, 0.05);
        }}
        .summary {{
            font-style: normal;
            background-color: #f6f9fc;
            padding: 1.5rem;
            border-radius: 4px;
            margin: 1.5rem 0;
            border: 1px solid #d8e1e8;
        }}
        p {{
            margin: 0.80rem 0;
            text-align: justify;
        }}
        @media print {{
            body {{
                padding: 1cm;
                background: white;
                color: black;
                max-width: none;
            }}
            .metadata, .summary, .question, .answer {{
                border: 1px solid #ddd;
                page-break-inside: avoid;
            }}
            h1, h2, h3 {{
                page-break-after: avoid;
                font-size: 4rem;
                margin: 2rem 0;
                line-height: 1.5;
                padding: 0.75rem 0;
            }}
            p {{
                font-size: 1.2rem;
                line-height: 1.8;
            }}
        }}
    </style>
    """
    
    # Generate metadata section with formatted dates
    metadata = doc.metadata_obj
    
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
<html lang="fa" dir="rtl">
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
    <footer>
        <p><em>تاریخ صدور: {shamsi_date}</em></p>
    </footer>
</body>
</html>"""
    
    return html_content

async def save_html_file(doc: EdarehoquqyDocument, output_dir: Path, font_path: Optional[Path] = None) -> Path:
    """
    Save an EdarehoquqyDocument as an HTML file.
    
    Args:
        doc: The document to save
        output_dir: Directory to save the file in
        font_path: Optional path to the IranSans font file
        
    Returns:
        Path to the saved file
    """
    try:
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Generate filename from document ID or opinion number
        filename = f"{doc.id_edarehoquqy}.html"
        json_filename = f"{doc.id_edarehoquqy}.json"
        safe_filename = "".join(c if c.isalnum() or c in "._- " else "_" for c in filename)
        file_path = output_dir / safe_filename
        json_file_path = output_dir / json_filename
        # Generate HTML content
        html_content = await document_to_html(doc, font_path=font_path)
        
        # Save to file
        async with aiofiles.open(file_path, mode="w", encoding="utf-8") as file:
            await file.write(html_content)
        async with aiofiles.open(json_file_path, mode="w", encoding="utf-8") as file:
            await file.write(json.dumps(doc.model_dump(), ensure_ascii=False, indent=4))
            
        logger.info(f"Saved HTML file: {file_path}")
        return file_path
        
    except Exception as e:
        logger.error(f"Error saving HTML file for document {doc.id_edarehoquqy}: {e}")
        raise

async def export_documents_to_html(documents: List[EdarehoquqyDocument], output_dir: Optional[Path] = None, font_path: Optional[Path] = None) -> List[Path]:
    """
    Export multiple EdarehoquqyDocument objects to HTML files.
    
    Args:
        documents: List of documents to export
        output_dir: Directory to save files in (defaults to ~/edarehoquqy-html-output)
        font_path: Optional path to the IranSans font file
        
    Returns:
        List of paths to the saved files
    """
    if output_dir is None:
        output_dir = Path.home() / "edarehoquqy-html-output"
        
    # Default font path if not provided
    if font_path is None:
        font_path = Path(__file__).parent.parent / "assets" / "fonts" / "IranSans.woff2"
        if not font_path.exists():
            logger.warning(f"Default font file not found at {font_path}")
            font_path = None
        
    saved_files = []
    for doc in documents:
        try:
            file_path = await save_html_file(doc, output_dir, font_path)
            saved_files.append(file_path)
        except Exception as e:
            logger.error(f"Failed to export document {doc.id_edarehoquqy}: {e}")
            
    logger.info(f"Exported {len(saved_files)} HTML files to {output_dir}")
    return saved_files 