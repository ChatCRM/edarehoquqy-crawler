#!/usr/bin/env python3
import argparse
import asyncio
import json
import logging
import sys
from pathlib import Path
from openai import AsyncOpenAI
from typing import AsyncGenerator, List, Optional
import os

from src.csv_export import save_file, get_each_doc_summary, get_doc_title, EdarehoquqyDocument
from src.html_export import document_to_html, save_html_file
try:
    from src.elasticsearch_service import ElasticsearchService
except ImportError:
    ElasticsearchService = None
    logging.warning("ElasticsearchService not available. Some functionality will be limited.")

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def get_data(query_list: List[str], client: AsyncOpenAI = None) -> AsyncGenerator[EdarehoquqyDocument, None]:
    """
    Retrieve documents from Elasticsearch based on query list.
    
    Args:
        query_list: List of queries to search for
        client: Optional OpenAI client for generating titles and summaries
        
    Yields:
        EdarehoquqyDocument objects with data from Elasticsearch
    """
    if ElasticsearchService is None:
        logger.error("ElasticsearchService not available. Cannot retrieve documents from Elasticsearch.")
        return
        
    hosts = [os.getenv("ELASTICSEARCH_HOST")]
    password = os.getenv("ELASTICSEARCH_PASSWORD")
    username = os.getenv("ELASTICSEARCH_USERNAME")
    
    try:
        # Test connection before creating async context
        es_service = ElasticsearchService(hosts=hosts, password=password, username=username)
        
        async with es_service:
            for query in query_list:
                logger.info(f"Searching for documents matching query: '{query}'")
                
                try:
                    async for source in es_service.search_by_text_batch(query):
                        if source is not None:
                            # Create a document with basic info
                            doc = EdarehoquqyDocument(
                                id_edarehoquqy=source.get('id_edarehoquqy', ''),
                                question=source.get('question', ''),
                                answer=source.get('answer', ''),
                                title='',  # Will be populated later if needed
                                summary='',  # Will be populated later if needed
                                metdata=source.get('metadata', {})
                            )
                            
                            # Generate title and summary if client is provided
                            if client:
                                try:
                                    # Generate title
                                    doc_content = f"Question: {doc.question}\nAnswer: {doc.answer}"
                                    title_result = await get_doc_title(client, doc_content)
                                    doc.title = title_result.get('title', '')
                                    
                                    # Generate summary
                                    doc.summary = await get_each_doc_summary(client, doc_content)
                                    
                                    logger.info(f"Generated title and summary for document {doc.id_edarehoquqy}")
                                except Exception as e:
                                    logger.error(f"Error generating title/summary for {doc.id_edarehoquqy}: {e}")
                            
                            yield doc
                except Exception as e:
                    logger.error(f"Error searching for query '{query}': {e}")
                    continue
    except Exception as e:
        logger.error(f"Error connecting to Elasticsearch: {e}")
        return

async def load_file_as_document(file_path: Path, client: AsyncOpenAI = None) -> Optional[EdarehoquqyDocument]:
    """
    Load a JSON file and convert it to an EdarehoquqyDocument.
    
    Args:
        file_path: Path to the JSON file
        client: Optional OpenAI client for generating title and summary
        
    Returns:
        EdarehoquqyDocument if successful, None otherwise
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Create document object
        doc = EdarehoquqyDocument(
            id_edarehoquqy=data.get('id_edarehoquqy', ''),
            question=data.get('question', ''),
            answer=data.get('answer', ''),
            title=data.get('title', ''),
            summary=data.get('summary', ''),
            metdata=data.get('metdata', {})
        )
        
        # Generate title and summary if needed and client provided
        if client:
            if not doc.title:
                logger.info(f"Generating title for document from {file_path.name}")
                doc_content = f"Question: {doc.question}\nAnswer: {doc.answer}"
                title_result = await get_doc_title(client, doc_content)
                doc.title = title_result.get('title', '')
            
            if not doc.summary:
                logger.info(f"Generating summary for document from {file_path.name}")
                doc_content = f"{doc.question}\n{doc.answer}"
                doc.summary = await get_each_doc_summary(client, doc_content)
        
        return doc
    
    except Exception as e:
        logger.error(f"Error loading document from {file_path}: {e}")
        return None

async def convert_and_save_documents(query_list: List[str], output_dir: Path, client: AsyncOpenAI = None) -> List[Path]:
    """
    Convert and save documents retrieved from Elasticsearch to HTML files.
    
    Args:
        query_list: List of queries to search for in Elasticsearch
        output_dir: Directory to save HTML files to
        client: Optional AsyncOpenAI client for generating titles/summaries
        
    Returns:
        List of paths to saved HTML files
    """
    output_dir = Path(output_dir)
    os.makedirs(output_dir, exist_ok=True)
    
    saved_files = []
    document_counter = 0
    
    logger.info(f"Converting and saving documents for queries: {query_list}")
    
    # Process each document
    async for doc_data in get_data(query_list, client):
        document_counter += 1
        
        try:
            # Create query-specific subdirectory
            query_dir = output_dir / doc_data.id_edarehoquqy.replace('/', '_')
            os.makedirs(query_dir, exist_ok=True)
            
            # Save document as HTML
            html_path = await save_html_file(doc_data, query_dir)
            saved_files.append(html_path)
            
            logger.info(f"Saved document {document_counter} as HTML: {html_path}")
            
        except Exception as e:
            logger.error(f"Error processing document {doc_data.id_edarehoquqy}: {e}")
            import traceback
            logger.error(traceback.format_exc())
    
    if not saved_files:
        logger.warning("No documents were retrieved from Elasticsearch. Check your queries and connection settings.")
    
    logger.info(f"Converted and saved {len(saved_files)} documents to {output_dir}")
    return saved_files

async def convert_files(input_files: List[Path], output_dir: Path, client: AsyncOpenAI = None) -> List[Path]:
    """
    Convert JSON files to HTML documents.
    
    Args:
        input_files: List of input JSON file paths
        output_dir: Output directory for HTML files
        client: Optional OpenAI client for generating titles and summaries
        
    Returns:
        List of paths to saved HTML files
    """
    output_dir = Path(output_dir)
    os.makedirs(output_dir, exist_ok=True)
    
    saved_files = []
    
    for file_path in input_files:
        try:
            # Load document from file
            doc = await load_file_as_document(file_path, client)
            if not doc:
                continue
                
            # Create subdirectory based on filename
            file_dir = output_dir / file_path.stem
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
    parser = argparse.ArgumentParser(description="Convert and save Elasticsearch documents or JSON files to HTML")
    parser.add_argument("queries", nargs="*", help="Search queries to retrieve documents from Elasticsearch")
    parser.add_argument("-f", "--files", nargs="*", help="JSON files to convert to HTML")
    parser.add_argument("-o", "--output-dir", help="Output directory (default: ./html-output)",
                       default="./html-output")
    parser.add_argument("-t", "--generate-content", action="store_true",
                       help="Generate titles and summaries for documents using OpenAI")
    
    args = parser.parse_args()
    
    if not args.queries and not args.files:
        parser.error("Either search queries or input files must be provided")
    
    output_dir = Path(args.output_dir)
    
    # Initialize OpenAI client if generating content
    client = None
    if args.generate_content:
        logger.info("Initializing OpenAI client for content generation")
        client = AsyncOpenAI(
            base_url=os.getenv("OPENAI_BASE_URL"),
            api_key=os.getenv("OPENAI_API_KEY")
        )
    
    try:
        saved_files = []
        
        # Process files if provided
        if args.files:
            input_files = [Path(f) for f in args.files]
            file_output_dir = output_dir / "files"
            file_results = await convert_files(input_files, file_output_dir, client)
            saved_files.extend(file_results)
        
        # Process elasticsearch queries if provided
        if args.queries:
            es_output_dir = output_dir / "search"
            try:
                es_results = await convert_and_save_documents(args.queries, es_output_dir, client)
                saved_files.extend(es_results)
            except Exception as e:
                logger.error(f"Error retrieving documents from Elasticsearch: {e}")
                logger.error("If you're having connection issues, try using the --files option instead")
        
        # Print results
        logger.info(f"Successfully saved {len(saved_files)} HTML files")
        if saved_files:
            logger.info(f"Files saved to: {output_dir}")
    except Exception as e:
        logger.error(f"Error during conversion: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main()) 