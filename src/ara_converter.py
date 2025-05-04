#!/usr/bin/env python3
import argparse
import asyncio
import json
import logging
import sys
import os
from pathlib import Path
from typing import List, Dict, Tuple, Optional, AsyncGenerator, Set, Union, Any
from openai import AsyncOpenAI
import aiofiles
from aiopath import AsyncPath
from dotenv import load_dotenv
from pydantic import BaseModel, Field
import copy
import traceback
import base64

from src.csv_export import AraMetadata, AraDate, AraDocument, EdarehoquqyDocument, get_each_doc_summary, get_doc_title
from src.html_converter import get_query_keywords, QUERY_LIST_V1
from src.elasticsearch_service import ElasticsearchService
from src.word_export import WordExporter, Loader

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', filename="ara_converter.log")
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()


class AraLoader(Loader):
    def __init__(self, root_dir: str = "ara-search", elasticsearch_service: ElasticsearchService = None, openai_client: AsyncOpenAI = None):
        """
        Initialize a loader specifically for ARA index documents.
        
        Args:
            root_dir: Root directory for storing ARA documents
            elasticsearch_service: ElasticsearchService for ARA index
            openai_client: OpenAI client for generating titles and summaries
        """
        super().__init__(root_dir, elasticsearch_service, openai_client)

    async def load_json_file(self, dir_path: AsyncPath) -> AsyncGenerator[Optional[AraDocument], None]:
        """Load a JSON file from an ID directory and convert it to AraDocument"""
        try:
            json_files = []
            async for file in dir_path.glob("*.json"):
                json_files.append(file)
                
            if not json_files:
                logger.warning(f"No JSON files found in {dir_path}")
                return
                
            for json_file in json_files:
                # Use the first JSON file found
                try:
                    async with aiofiles.open(json_file, mode='r', encoding='utf-8') as f:
                        data = await f.read()
                        json_data = json.loads(data)
                        yield convert_to_ara_document(json_data)
                except json.JSONDecodeError:
                    logger.error(f"Failed to decode JSON from {json_file}")
                    self._failed_files.add(str(json_file))
                except Exception as e:
                    logger.error(f"Error loading file {json_file}: {e}")
                    self._failed_files.add(str(json_file))
        except Exception as e:
            logger.error(f"Error accessing directory {dir_path}: {e}")
            self._failed_files.add(str(dir_path))
            
    async def get_documents_by_keyword(self, keyword: str, index_type: str = "ara") -> AsyncGenerator[AraDocument, None]:
        """Get all documents for a specific keyword"""
        id_dirs = await self.get_id_dirs(keyword)
        logger.info(f"Found {len(id_dirs)} ID directories for keyword '{keyword}'")
        doc_count = 0
        
        for dir_path in id_dirs:
            documents_found = False
            async for document in self.load_json_file(dir_path):
                if document:
                    documents_found = True
                    # Add to categorized IDs set
                    self._categorized_ids.add(document.id_ara)
                    doc_count += 1
                    if doc_count % 10 == 0:
                        logger.info(f"Processed {doc_count} documents so far for keyword '{keyword}'")
                    yield document
            
            if not documents_found:
                logger.warning(f"Failed to load document from directory: {dir_path}")
                self._failed_ids.add(dir_path.name)
        
        logger.info(f"Total documents found for keyword '{keyword}': {doc_count}")

    async def get_uncategorized_documents(self, index_type: str = "ara") -> AsyncGenerator[AraDocument, None]:
        """
        Fetch documents from ARA index that aren't in any existing category.
        Override to handle ARA-specific document structure.
        """
        try:
            # Get all existing IDs from the root directory
            existing_ids = await self.get_all_existing_ids(str(self.root_dir))
            logger.info(f"Existing IDs: {len(existing_ids)}")
            
            # Create an uncategorized directory if it doesn't exist
            uncategorized_dir = AsyncPath(os.path.join(str(self.root_dir), "بدون-دسته-بندی"))
            await uncategorized_dir.mkdir(exist_ok=True, parents=True)
            
            # Process each uncategorized document
            processed_count = 0
            
            # Pass the index_name parameter properly
            async for doc in self.elasticsearch_service.get_missing_documents(existing_ids, index_name="ara"):
                try:
                    if not isinstance(doc, dict):
                        logger.warning(f"Skipping invalid document: {doc}")
                        continue
                        
                    # Check for id_ara instead of id_edarehoquqy
                    if "id_ara" not in doc:
                        logger.warning(f"Document missing ID field: {doc}")
                        continue
                    
                    # Convert to AraDocument object
                    document = convert_to_ara_document(doc)
                    
                    # Generate summary if needed
                    if not document.summary or document.summary == "":
                        for attempt in range(3):
                            try:
                                # Use the entire document content for summary
                                content_for_summary = f"{document.title or ''}\n{document.message or ''}\n{document.content or ''}"
                                if summary := await get_each_doc_summary(self.openai_client, content_for_summary):
                                    document.summary = summary
                                    break
                            except Exception as e:
                                logger.warning(f"Error getting summary for document {document.id_ara} (attempt {attempt+1}/3): {e}")
                                if attempt < 2:
                                    await asyncio.sleep(2)
                    
                    # Save the document to the uncategorized directory
                    doc_dir = uncategorized_dir / document.id_ara
                    await doc_dir.mkdir(exist_ok=True)
                    
                    # Save JSON file
                    json_path = doc_dir / f"{document.id_ara}.json"
                    
                    # Manually create a serializable dictionary
                    serialized_doc = {
                        "id_ara": document.id_ara,
                        "title": document.title,
                        "content": document.content,
                        "message": document.message,
                        "documents": document.documents,
                        "summary": document.summary,
                        "date": {
                            "georgian": document.date.georgian if document.date and document.date.georgian else "0001/01/01",
                            "gregorian": document.date.gregorian if document.date and document.date.gregorian else "0001/01/01", 
                            "shamsi": document.date.shamsi if document.date and document.date.shamsi else "0001/01/01"
                        } if document.date else {"georgian": "0001/01/01", "gregorian": "0001/01/01", "shamsi": "0001/01/01"},
                        "metadata": {
                            "authority_type": document.metadata.authority_type if document.metadata else None,
                            "branch": document.metadata.branch if document.metadata else None,
                            "final_judgment": document.metadata.final_judgment if document.metadata else None,
                            "judge": document.metadata.judge if document.metadata else None,
                            "judgment_group": document.metadata.judgment_group if document.metadata else None,
                            "judgment_type": document.metadata.judgment_type if document.metadata else None,
                            "petition_date": document.metadata.petition_date if document.metadata else None,
                            "selected_document_judgment": document.metadata.selected_document_judgment if document.metadata else None,
                            "session_number": document.metadata.session_number if document.metadata else None
                        } if document.metadata else None
                    }
                    
                    async with aiofiles.open(json_path, mode="w", encoding="utf-8") as file:
                        await file.write(json.dumps(serialized_doc, ensure_ascii=False, indent=4))
                    
                    # Save custom HTML file for ARA documents
                    try:
                        font_path = Path(__file__).parent.parent / "assets" / "fonts" / "IRANSans.woff2"
                        html_content = await create_ara_html(document, font_path if font_path.exists() else None)
                        html_path = doc_dir / f"{document.id_ara}.html"
                        async with aiofiles.open(html_path, mode="w", encoding="utf-8") as file:
                            await file.write(html_content)
                    except Exception as e:
                        logger.error(f"Error saving HTML file for uncategorized document {document.id_ara}: {e}")
                    
                    processed_count += 1
                    if processed_count % 10 == 0:
                        logger.info(f"Processed {processed_count} uncategorized documents so far")
                    
                    yield document
                except Exception as e:
                    logger.warning(f"Error processing uncategorized document: {e}")
                    logger.warning(f"Error details: {str(e)}")
                    continue

        except Exception as e:
            logger.error(f"Error loading uncategorized documents: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")

def convert_to_ara_document(doc_data: Dict[str, Any]) -> AraDocument:
    """
    Convert a raw document dictionary to an AraDocument instance.
    
    Args:
        doc_data: Raw document data from Elasticsearch
        
    Returns:
        AraDocument instance
    """
    try:
        # Extract date information
        date = None
        if "date" in doc_data:
            try:
                # Create a validated date structure with default values for None
                date_dict = {
                    "georgian": doc_data["date"].get("georgian", "0001/01/01") or "0001/01/01",
                    "gregorian": doc_data["date"].get("gregorian", "0001/01/01") or "0001/01/01",
                    "shamsi": doc_data["date"].get("shamsi", "0001/01/01") or "0001/01/01"
                }
                date = AraDate(**date_dict)
            except Exception as e:
                logger.warning(f"Error creating AraDate from data: {e}. Using default values.")
                date = AraDate()
        
        # Extract metadata
        metadata = None
        if "metadata" in doc_data:
            try:
                # Pre-process selected_document_judgment to ensure it's properly handled
                metadata_dict = dict(doc_data["metadata"])
                
                # Handle None for selected_document_judgment
                if "selected_document_judgment" in metadata_dict and metadata_dict["selected_document_judgment"] is None:
                    metadata_dict["selected_document_judgment"] = []
                
                # Keep it as a list or convert to string if needed
                metadata = AraMetadata(**metadata_dict)
            except Exception as e:
                logger.warning(f"Error creating AraMetadata from data: {e}. Using default values.")
                metadata = AraMetadata()
        
        # Create the document - preserving the title from Elasticsearch
        return AraDocument(
            id_ara=doc_data.get("id_ara", ""),
            title=doc_data.get("title", ""),  # Use the title from Elasticsearch
            content=doc_data.get("content", ""),
            message=doc_data.get("message", ""),
            documents=doc_data.get("documents", ""),
            summary=doc_data.get("summary", ""),
            date=date,
            metadata=metadata
        )
    except Exception as e:
        logger.error(f"Error converting document data to AraDocument: {e}")
        logger.error(f"Document data: {doc_data}")
        # Create a minimal valid document
        return AraDocument(id_ara=doc_data.get("id_ara", "unknown_id"))


async def get_all_existing_ara_ids(output_dir: str) -> Dict[str, AsyncPath]:
    """
    Get all existing ARA document IDs from the output directory.
    
    Args:
        output_dir: Path to the output directory
        
    Returns:
        Dict of existing document IDs and their corresponding file paths
    """
    existing_ids = {}
    output_path = AsyncPath(output_dir)
    
    if not await output_path.exists():
        logger.info(f"Output directory {output_dir} does not exist")
        return existing_ids
        
    # First look for files directly in keyword subdirectories
    async for keyword_dir in output_path.glob('*'):
        if not await keyword_dir.is_dir() or keyword_dir.name.startswith('.'):
            continue
            
        logger.info(f"Scanning keyword directory: {keyword_dir}")
        async for id_dir in keyword_dir.glob('*'):
            if not await id_dir.is_dir() or id_dir.name.startswith('.'):
                continue
                
            # Extract ID from directory name
            id_ara = id_dir.name
            if id_ara not in existing_ids:
                existing_ids[id_ara] = id_dir
                
    # Also check the uncategorized directory if it exists
    uncategorized_dir = AsyncPath(os.path.join(output_dir, "بدون-دسته-بندی"))
    if await uncategorized_dir.exists():
        logger.info(f"Scanning uncategorized directory: {uncategorized_dir}")
        async for id_dir in uncategorized_dir.glob('*'):
            if not await id_dir.is_dir() or id_dir.name.startswith('.'):
                continue
                
            # Extract ID from directory name
            id_ara = id_dir.name
            if id_ara not in existing_ids:
                existing_ids[id_ara] = id_dir
    
    logger.info(f"Found {len(existing_ids)} existing ARA document IDs")
    return existing_ids


async def get_data_from_ara(query_list: List[str], client: AsyncOpenAI, output_dir: str = "../ara-output", max_results: int = None) -> AsyncGenerator[Tuple[AraDocument, str], None]:
    """
    Retrieve documents from Elasticsearch ARA index based on query list.
    
    Args:
        query_list: List of queries to search for
        client: OpenAI client for generating titles and summaries
        output_dir: Directory to save output files
        
    Yields:
        AraDocument objects with data from Elasticsearch + Query in a Tuple
    """
    
    es_service = None
    
    try:
        hosts = os.getenv("ELASTICSEARCH_HOST")
        password = os.getenv("ELASTICSEARCH_PASSWORD")
        username = os.getenv("ELASTICSEARCH_USERNAME")

        processed_files = set()
        existing_ids = await get_all_existing_ara_ids(output_dir)
        
        # Create ES service
        es_service = ElasticsearchService(
            hosts=[hosts], 
            password=password, 
            username=username,
            index_name="ara",  # Use ARA index
            timeout=120,
            retry_on_timeout=True,
            max_retries=5
        )
        
        # Ensure index exists
        await es_service.ensure_index_exists()
        
        for query_text in query_list:
            logger.info(f"Searching ARA index for documents matching query: '{query_text}'")
            
            try:
                document_count = 0
                # Generate keywords for better search quality (like in html_converter)
                query_phrases = await get_query_keywords(client, query_text)
                logger.info(f"Generated {len(query_phrases)} keywords for query: {query_text}")
                logger.info(f"Keywords: {query_phrases}")
                
                # Adjust min_score based on number of phrases
                num_phrases = len(query_phrases) if isinstance(query_phrases, list) else 1
                min_score = 1.5  # Base value
                if num_phrases > 5:
                    min_score = 2.0
                elif num_phrases > 2:
                    min_score = 2.5
                
                
                # Execute search
                async for batch in es_service.search_by_text_batch(
                    query_phrases, 
                    min_score=min_score, 
                    max_results=max_results,
                ):
                    if batch is None:
                        continue
                        
                    results = batch.get("results", [])
                    relevant_count = batch.get("relevant_count", len(results))
                    total_count = batch.get("total_count", 0)
                    precision = batch.get("precision", 0)
                    
                    logger.info(f"Processing batch with {len(results)} results (relevant: {relevant_count} of {total_count} total, precision: {precision:.1%})")
                    
                    for source in results:
                        document_count += 1
                        doc_id = source.get('id_ara')
                        if not doc_id:
                            logger.warning(f"Missing id_ara in document: {source.keys()}")
                            continue
                            
                        if doc_id in existing_ids:
                            logger.info(f"Already processed file with ID: {doc_id}")
                            doc_path = existing_ids[doc_id] / doc_id
                            if doc := await check_if_document_exists(doc_path, doc_id):
                                logger.info(f"Loaded document from {doc_path}")
                                yield doc, query_text
                                continue

                        # Convert to AraDocument
                        doc = convert_to_ara_document(source)
                        
                        # Generate title only if it's missing from Elasticsearch
                        if not doc.title or doc.title == "":
                            content_for_title = f"{doc.message or ''}\n{doc.content or ''}"
                            doc.title = await get_doc_title(client, content_for_title)
                            logger.info(f"Generated title for document {doc.id_ara}")
                        else:
                            logger.info(f"Using existing title from Elasticsearch for document {doc.id_ara}")
                        
                        # Generate summary if needed
                        if not doc.summary or doc.summary == "":
                            content_for_summary = f"{doc.title or ''}\n{doc.message or ''}\n{doc.content or ''}"
                            doc.summary = await get_each_doc_summary(client, content_for_summary)
                        
                        processed_files.add(doc.id_ara)
                        yield doc, query_text
                        
                logger.info(f"Processed {document_count} documents for query: '{query_text}'")
                if document_count == 0:
                    logger.warning(f"No documents found for query: '{query_text}' in ARA index.")
                    
            except Exception as e:
                logger.error(f"Error searching for query '{query_text}' in ARA index: {e}")
                logger.error(f"Exception details: {str(e)}")
                continue
    except Exception as e:
        logger.error(f"Error connecting to Elasticsearch for ARA index: {e}")
        logger.error(f"Exception details: {str(e)}")
    finally:
        if es_service:
            try:
                await es_service.close()
                logger.info("Closed Elasticsearch connection")
            except Exception as e:
                logger.error(f"Error closing Elasticsearch connection: {e}")

async def check_if_document_exists(output_dir: AsyncPath, id_ara: str) -> Union[bool, AraDocument]:
    """
    Check if a document with the given ID exists in the output directory.
    
    Args:
        output_dir: Path to the output directory
        id_ara: ID of the document to check
        
    Returns:
        AraDocument if the document exists, False otherwise
    """
    if not await output_dir.exists():
        return False
    json_file = output_dir / f"{id_ara}.json"
    if not await json_file.exists():
        return False
    try:
        async with aiofiles.open(json_file, mode='r', encoding='utf-8') as f:
            data = await f.read()
        doc_data = json.loads(data)
        return convert_to_ara_document(doc_data)
    except Exception as e:
        logger.error(f"Error loading document from {json_file}: {e}")
        return False

async def save_ara_document(doc: AraDocument, output_dir: Path, font_path: Optional[Path] = None) -> Tuple[Path, Path]:
    """
    Save an AraDocument as HTML and JSON files.
    
    Args:
        doc: The document to save
        output_dir: Directory to save the files
        font_path: Optional path to a font file
        
    Returns:
        Tuple of paths to the saved HTML and JSON files
    """
    try:
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Create custom HTML to include message section
        html_content = await create_ara_html(doc, font_path=font_path)
        
        # Save HTML file
        html_path = output_dir / f"{doc.id_ara}.html"
        async with aiofiles.open(html_path, mode='w', encoding='utf-8') as f:
            await f.write(html_content)
            
        # Manually create a serializable dictionary
        serialized_doc = {
            "id_ara": doc.id_ara,
            "title": doc.title,
            "content": doc.content,
            "message": doc.message,
            "documents": doc.documents,
            "summary": doc.summary,
            "date": {
                "georgian": doc.date.georgian if doc.date and doc.date.georgian else "0001/01/01",
                "gregorian": doc.date.gregorian if doc.date and doc.date.gregorian else "0001/01/01", 
                "shamsi": doc.date.shamsi if doc.date and doc.date.shamsi else "0001/01/01"
            } if doc.date else {"georgian": "0001/01/01", "gregorian": "0001/01/01", "shamsi": "0001/01/01"},
            "metadata": {
                "authority_type": doc.metadata.authority_type,
                "branch": doc.metadata.branch,
                "final_judgment": doc.metadata.final_judgment,
                "judge": doc.metadata.judge,
                "judgment_group": doc.metadata.judgment_group,
                "judgment_type": doc.metadata.judgment_type,
                "petition_date": doc.metadata.petition_date,
                "selected_document_judgment": doc.metadata.selected_document_judgment,
                "session_number": doc.metadata.session_number
            } if doc.metadata else None
        }
            
        # Save JSON file
        json_path = output_dir / f"{doc.id_ara}.json"
        async with aiofiles.open(json_path, mode='w', encoding='utf-8') as f:
            await f.write(json.dumps(serialized_doc, ensure_ascii=False, indent=2))
        
        return html_path, json_path
    
    except Exception as e:
        logger.error(f"Error saving document {doc.id_ara}: {e}")
        raise

async def create_ara_html(doc: AraDocument, font_path: Optional[Path] = None) -> str:
    """
    Create HTML content for an AraDocument with message section included.
    
    Args:
        doc: The document to convert to HTML
        font_path: Optional path to a font file
        
    Returns:
        HTML string
    """
    # Font embedding
    font_face = ""
    if font_path and font_path.exists():
        try:
            async with aiofiles.open(font_path, mode="rb") as font_file:
                font_data = await font_file.read()
                font_base64 = base64.b64encode(font_data).decode("utf-8")
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
    
    # Basic styling
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
        .title-section {{
            font-weight: 600;
            margin-top: 2rem;
            padding: 1rem;
            border-radius: 4px;
        }}
        .message-section {{
            font-weight: 600;
            margin-top: 2rem;
            background-color: #fff8f1;
            padding: 1rem;
            border-right: 4px solid #a57a4a;
            border-radius: 4px;
        }}
        .content-section {{
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
        .section-content {{
            padding: 0.5rem 1rem;
            margin-bottom: 1.5rem;
            background-color: #fff;
            border: 1px solid #eaeaea;
            border-radius: 4px;
        }}
        .section-heading {{
            font-size: 1.3em;
            font-weight: 600;
            color: #2c3e50;
            background-color: #f1f8ff;
            padding: 0.7rem 1rem;
            border-right: 4px solid #4a6fa5;
            border-radius: 4px;
            margin: 1.5rem 0 0.5rem 0;
        }}
        @media print {{
            body {{
                padding: 1cm;
                background: white;
                color: black;
                max-width: none;
            }}
            .metadata, .summary, .title-section, .message-section, .content-section, .section-content {{
                border: 1px solid #ddd;
                page-break-inside: avoid;
            }}
            h1, h2, h3, .section-heading {{
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
    metadata_html = "<div class='metadata'><h3>اطلاعات سند</h3>"
    metadata_html += f"<p><strong>شناسه سند:</strong> {doc.id_ara}</p>"
    
    # Add authority type if available
    if doc.metadata and doc.metadata.authority_type:
        metadata_html += f"<p><strong>نوع مرجع:</strong> {doc.metadata.authority_type}</p>"
    
    # Add judgment type if available
    if doc.metadata and doc.metadata.judgment_type:
        metadata_html += f"<p><strong>نوع حکم:</strong> {doc.metadata.judgment_type}</p>"
    
    # Add judgment group if available and not the default
    if doc.metadata and doc.metadata.judgment_group and doc.metadata.judgment_group != "نامشخص":
        metadata_html += f"<p><strong>گروه حکم:</strong> {doc.metadata.judgment_group}</p>"
    
    # Add selected document judgment if available, handling both array and string values
    if doc.metadata and doc.metadata.selected_document_judgment:
        if isinstance(doc.metadata.selected_document_judgment, list) and doc.metadata.selected_document_judgment:
            # Join array items with commas
            judgments = " , ".join(doc.metadata.selected_document_judgment)
            metadata_html += f"<p><strong>مراجع قضایی:</strong> {judgments}</p>"
        elif isinstance(doc.metadata.selected_document_judgment, str) and doc.metadata.selected_document_judgment.strip():
            # Single string value
            metadata_html += f"<p><strong>مراجع قضایی:</strong> {doc.metadata.selected_document_judgment}</p>"
    
    # Add branch if available
    if doc.metadata and doc.metadata.branch and doc.metadata.branch.strip():
        metadata_html += f"<p><strong>شعبه:</strong> {doc.metadata.branch}</p>"
    
    # Add judge if available
    if doc.metadata and doc.metadata.judge and doc.metadata.judge.strip():
        metadata_html += f"<p><strong>قاضی:</strong> {doc.metadata.judge}</p>"
    
    # Add session number if available and not the default
    if doc.metadata and doc.metadata.session_number and doc.metadata.session_number != "نامشخص":
        metadata_html += f"<p><strong>شماره جلسه:</strong> {doc.metadata.session_number}</p>"
    
    # Add date information if available
    if doc.date:
        if doc.date.shamsi and doc.date.shamsi != "0001/01/01":
            metadata_html += f"<p><strong>تاریخ شمسی:</strong> {doc.date.shamsi}</p>"
        if doc.date.gregorian and doc.date.gregorian != "0001/01/01":
            metadata_html += f"<p><strong>تاریخ میلادی:</strong> {doc.date.gregorian}</p>"
    
    metadata_html += "</div>"
    
    # Generate summary section if available
    summary_html = ""
    if doc.summary:
        summary_html = f"""
        <div class="summary">
            <h3>خلاصه</h3>
            <p>{doc.summary}</p>
        </div>
        """
    
    # Process content with ## separators
    content_html = ""
    if doc.content:
        # Split content by ##
        sections = doc.content.split("##")
        
        if len(sections) > 1:
            # The first section might be empty if the content starts with ##
            if sections[0].strip():
                content_html += f'<div class="section-content"><p>{sections[0].strip()}</p></div>'
            
            # Process each section after the first
            for i in range(1, len(sections)):
                section = sections[i].strip()
                if not section:
                    continue
                
                # Extract the heading (first line) and content
                lines = section.split('\n', 1)
                heading = lines[0].strip()
                
                content_html += f'<div class="section-heading">{heading}</div>'
                
                if len(lines) > 1:
                    section_content = lines[1].strip()
                    content_html += f'<div class="section-content"><p>{section_content}</p></div>'
        else:
            # No ## found, treat as regular content
            content_html = f'<div class="section-content"><p>{doc.content}</p></div>'
    
    # Generate message section if available
    message_html = ""
    if doc.message:
        message_html = f"""
        <div class="message-section">
            <h2>پیام</h2>
            <p>{doc.message}</p>
        </div>
        """
    
    # Use title or ID for the page title
    page_title = doc.title if doc.title else f"سند {doc.id_ara}"
    
    # Title should be at the top
    title_html = f"""
    <h1>{page_title}</h1>
    """
    
    # Assemble the complete HTML document with title at the top
    html_content = f"""<!DOCTYPE html>
<html lang="fa" dir="rtl">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{page_title}</title>
    {styles}
</head>
<body>
    {title_html}
    {metadata_html}
    {summary_html}
    <div class="content-section">
        {content_html}
    </div>
    {message_html}
    <hr>
    <footer>
        <p><em>شناسه سند: {doc.id_ara}</em></p>
    </footer>
</body>
</html>"""
    
    return html_content

async def convert_and_save_ara_documents(exporter: WordExporter, query_list: List[str], output_dir: Path, client: AsyncOpenAI = None) -> List[Path]:
    """
    Convert and save documents from ARA index to HTML files.
    
    Args:
        query_list: List of queries to search for
        output_dir: Directory to save HTML files to
        client: AsyncOpenAI client for generating titles/summaries
        
    Returns:
        List of paths to saved HTML files
    """
    output_dir = Path(output_dir)
    os.makedirs(output_dir, exist_ok=True)
    
    # Get IranSans font path
    font_path = Path(__file__).parent.parent / "assets" / "fonts" / "IRANSans.woff2"
    if not font_path.exists():
        logger.warning(f"IranSans font not found at {font_path}, HTML will use fallback fonts")
        font_path = None
    else:
        logger.info(f"Using IranSans font from {font_path}")
    
    saved_files = []
    document_counter = 0
    failed_ids = set()
    
    logger.info(f"Converting and saving ARA documents for queries: {query_list}")
    
    try:
        # Process each document
        previous_query = query_list[0]
        async for doc_data, query in get_data_from_ara(query_list, client, str(output_dir), max_results=4):
            document_counter += 1
            
            try:
                # Create query-specific subdirectory
                query_dir = output_dir / query.replace('/', '_').replace(' ', '_') / doc_data.id_ara.replace('/', '_')
                os.makedirs(query_dir, exist_ok=True)
                
                # Save document as HTML and JSON
                html_path, _ = await save_ara_document(doc_data, query_dir, font_path=font_path)
                
                saved_files.append(html_path)
                logger.info(f"Saved document {document_counter} as HTML: {html_path}")

                # Export Word documents for each new query we encounter
                if previous_query != query:
                    logger.info(f"Exporting Word documents for query: {query}")
                    word_paths = await exporter.export_documents_by_keyword(query, index_type="ara")
                    if word_paths:
                        logger.info(f"Successfully exported Word documents for '{query}': {word_paths}")
                    else:
                        logger.warning(f"Failed to export Word documents for '{query}'")
                    previous_query = query
                
            except Exception as e:
                logger.error(f"Error processing document {doc_data.id_ara}: {e}")
                failed_ids.add(doc_data.id_ara)
                import traceback
                logger.error(traceback.format_exc())
        
        if previous_query != query_list[-1]:
            logger.info(f"Exporting Word documents for query: {query_list[-1]}")
            word_paths = await exporter.export_documents_by_keyword(query_list[-1], index_type="ara")
            if word_paths:
                logger.info(f"Successfully exported Word documents for '{query_list[-1]}': {word_paths}")
            else:
                logger.warning(f"Failed to export Word documents for '{query_list[-1]}'")
        
        if previous_query == query_list[0]:
            logger.info(f"Exporting Word documents for query: {query_list[0]}")
            word_paths = await exporter.export_documents_by_keyword(query_list[0], index_type="ara")
            if word_paths:
                logger.info(f"Successfully exported Word documents for '{query_list[0]}': {word_paths}")
            else:
                logger.warning(f"Failed to export Word documents for '{query_list[0]}'")
        
        if not saved_files:
            logger.warning("No documents were retrieved from ARA index. Check your queries and connection settings.")
        
            
        
        # Save failed IDs to a file for later reference
        if failed_ids:
            failed_ids_path = output_dir / "failed_ids.txt"
            async with aiofiles.open(failed_ids_path, mode='w', encoding='utf-8') as f:
                await f.write("\n".join(failed_ids))
            logger.warning(f"Failed to process {len(failed_ids)} documents. IDs saved to {failed_ids_path}")
        
        logger.info(f"Converted and saved {len(saved_files)} documents to {output_dir}")
        return saved_files
    except Exception as e:
        logger.error(f"Error retrieving documents from ARA index: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return saved_files

async def process_ara_index(query_list: List[str], output_base_dir: str = None, limit: int = None) -> None:
    """
    Main function to process ARA index - search, convert to HTML, and export to Word.
    
    Args:
        query_list: List of queries to search
        output_base_dir: Base directory for output files (default is current directory)
    """
    if not output_base_dir:
        output_base_dir = os.getcwd()
    
    # Prepare directories
    html_output_dir = os.path.join(output_base_dir, "ara-search")
    word_output_dir = os.path.join(output_base_dir, "ara-word")
    
    os.makedirs(html_output_dir, exist_ok=True)
    os.makedirs(word_output_dir, exist_ok=True)
    
    # Initialize OpenAI client
    if os.getenv("OPENAI_API_KEY") is None or os.getenv("OPENAI_BASE_URL") is None:
        logger.error("OPENAI_API_KEY or OPENAI_BASE_URL is not set. Please set in your environment variables.")
        sys.exit(1)
    
    client = AsyncOpenAI(
        base_url=os.getenv("OPENAI_BASE_URL"),
        api_key=os.getenv("OPENAI_API_KEY")
    )
    
    # Initialize Elasticsearch service for ARA index
    hosts = [os.getenv("ELASTICSEARCH_HOST")]
    username = os.getenv("ELASTICSEARCH_USERNAME")
    password = os.getenv("ELASTICSEARCH_PASSWORD")
    
    # Check for required environment variables
    es_env_vars = ["ELASTICSEARCH_HOST", "ELASTICSEARCH_USERNAME", "ELASTICSEARCH_PASSWORD"]
    missing_vars = [var for var in es_env_vars if not os.getenv(var)]
    if missing_vars:
        logger.error(f"Missing required environment variables: {', '.join(missing_vars)}")
        sys.exit(1)
    
    try:
        # Create uncategorized directory structure
        uncategorized_dir = os.path.join(html_output_dir, "بدون-دسته-بندی")
        os.makedirs(uncategorized_dir, exist_ok=True)
        
        async with ElasticsearchService(
            hosts=hosts, 
            username=username, 
            password=password, 
            index_name="ara"
        ) as es:
            # Create AraLoader instance
            loader = AraLoader(html_output_dir, es, client)
            # Create WordExporter
            exporter = WordExporter(loader, word_output_dir)

            # Step 1: Convert documents to HTML
            logger.info(f"Starting conversion of ARA documents to HTML for queries: {query_list}")
            saved_files = await convert_and_save_ara_documents(exporter, query_list, html_output_dir, client)
            logger.info(f"Saved {len(saved_files)} HTML files")
        
            # Initialize services
            await es.ensure_index_exists()
            
            # Export uncategorized documents
            logger.info("Exporting uncategorized documents")
            uncategorized_paths = await exporter.export_uncategorized_documents(index_type="ara")
            
            if uncategorized_paths:
                logger.info(f"Successfully exported uncategorized documents to {len(uncategorized_paths)} file(s):")
                for i, path in enumerate(uncategorized_paths):
                    logger.info(f"  - Part {i+1}: {path}")
            else:
                logger.info("No uncategorized documents found")
            
            # Log any failures
            async with aiofiles.open(os.path.join(word_output_dir, "ara_export_status.txt"), mode="w", encoding="utf-8") as file:
                failed_ids = loader.failed_ids
                if failed_ids:
                    logger.warning(f"Failed to load {len(failed_ids)} document IDs")
                    await file.write(f"Failed to load {len(failed_ids)} document IDs: {failed_ids}\n")
                
                failed_files = loader.failed_files
                if failed_files:
                    logger.warning(f"Failed to load {len(failed_files)} files")
                    await file.write(f"Failed to load {len(failed_files)} files: {failed_files}\n")
    
    except Exception as e:
        logger.error(f"Error during processing: {e}")
        import traceback
        logger.error(traceback.format_exc())

async def main():
    parser = argparse.ArgumentParser(description="Convert and save documents from ARA index to HTML and Word files")
    parser.add_argument("-q", "--queries", nargs="*", default=None, 
                        help="Search queries to retrieve documents from ARA index")
    parser.add_argument("-o", "--output-dir", help="Base output directory (default: ./)",
                       default="../")
    parser.add_argument("--clear-existing", action="store_true", help="Clear existing output directories before processing")
    
    args = parser.parse_args()
    
    query_list = args.queries if args.queries else QUERY_LIST_V1[:5]
    
    # Clear existing directories if requested
    if args.clear_existing:
        html_dir = os.path.join(args.output_dir, "ara-search")
        word_dir = os.path.join(args.output_dir, "ara-word")
        
        if os.path.exists(html_dir):
            logger.info(f"Clearing existing HTML directory: {html_dir}")
            import shutil
            shutil.rmtree(html_dir)
            
        if os.path.exists(word_dir):
            logger.info(f"Clearing existing Word directory: {word_dir}")
            import shutil
            shutil.rmtree(word_dir)
    
    try:
        # Process the ARA index
        await process_ara_index(query_list, args.output_dir)
        logger.info("ARA index processing completed successfully!")
    except Exception as e:
        logger.error(f"Error in main process: {e}")
        logger.error(traceback.format_exc())
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main()) 