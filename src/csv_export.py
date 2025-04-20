from elasticsearch import AsyncElasticsearch
import logging
import aiofiles
import asyncio
import os
import json
import io
import csv
from pathlib import Path
from dotenv import load_dotenv
from openai import AsyncOpenAI

from src.elasticsearch_service import ElasticsearchService

load_dotenv()

total_ids = set()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def get_doc_title(client: AsyncOpenAI, doc: str) -> dict:
    """
    Generate a concise, descriptive title (5-6 words) for a legal document in Persian.
    
    Args:
        client: AsyncOpenAI client instance
        doc: The document text to generate a title for
        
    Returns:
        A dictionary containing the generated title
    """
    prompt = f"""
    Create a precise Persian title (10-11 words) for this Iranian legal document that captures its core legal issue.
    
    The title should:
    - Be in Persian
    - Be in 10-11 words
    - Use precise Iranian legal terminology
    - Be specific enough to distinguish this document
    - Be formatted as JSON with only a "title" field
    
    Document:
    {doc}
    """
    
    response = await client.chat.completions.create(
        model="gpt-4o-mini",  # Using the more capable model for better title generation
        messages=[{"role": "user", "content": prompt}],
        response_format={"type": "json_object"},
        temperature=0.3  # Lower temperature for more focused, precise titles
    )
    
    return json.loads(response.choices[0].message.content)

async def get_each_doc_summary(client: AsyncOpenAI, doc: str) -> list:
    """
    Get each document summary from Elasticsearch and return a list of documents.
    """
    prompt = f"""
        You are a helpful assistant that summarizes documents.
        Documents provided are about Iranian Documents and law disputes and discussions.
        The output of your summary should be in Persian and will be read by Lawyers and Experts in the field.
        Education level of the audience is Master and PhD and your summary should be in a way that is easy to understand for them and capture the main points.
        Please summarize the following document:
        {doc}
        """
    response = await client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
    )
    return response.choices[0].message.content

async def get_data(es: ElasticsearchService, index_name: str, query: str, max_results: int = None) -> list:
    """
    Get data from Elasticsearch and return a list of documents.
    """
    try:
        logger.info(f"Searching for: '{query}' with max_results={max_results}")
        response = await es.search_by_text(query, max_results=max_results)
        logger.info(f"Found {len(response)} documents for query: '{query}'")

        if not response:
            logger.warning(f"No data found for query: {query}")
            return []
        
        
            
        # Process each document to add URL
        for doc in response:
            if 'id_edarehoquqy' in doc and doc['id_edarehoquqy']:
                id_field = 'id_edarehoquqy'
                file_id = doc[id_field]['file_id'] if isinstance(doc[id_field], dict) else doc[id_field]
                if file_id:
                    total_ids.add(file_id)
            elif 'id_ghavanin' in doc and doc['id_ghavanin']:
                id_field = 'id_ghavanin'
                file_id = doc[id_field]['file_id'] if isinstance(doc[id_field], dict) else doc[id_field]
                if file_id:
                    total_ids.add(file_id)
            else:
                doc['url'] = "https://edarehoquqy.eadl.ir/"
                continue
                
            # Create URL
            doc['url'] = f"https://edarehoquqy.eadl.ir/%D8%AC%D8%B3%D8%AA%D8%AC%D9%88%DB%8C-%D9%86%D8%B8%D8%B1%DB%8C%D8%A7%D8%AA-%D9%85%D8%B4%D9%88%D8%B1%D8%AA%DB%8C/%D8%AC%D8%B3%D8%AA%D8%AC%D9%88%DB%8C-%D9%86%D8%B8%D8%B1%DB%8C%D9%87/moduleId/1286/controller/Search/action/Detail?IdeaId={file_id}"
            
            # Remove ID field
            if id_field in doc:
                del doc[id_field]
        return response
    except Exception as e:
        logger.error(f"Error getting data from Elasticsearch: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return []

def export_to_csv(data: list) -> str:
    """
    Export data to CSV string
    """
    try:
        if not data:
            return ""
            
        output = io.StringIO()
        if data:
            fields = data[0].keys()
            writer = csv.DictWriter(output, fieldnames=fields)
            writer.writeheader()
            writer.writerows(data)
        return output.getvalue()
    except Exception as e:
        logger.error(f"Error exporting to CSV: {e}")
        raise

async def export_to_text(data: list, client: AsyncOpenAI) -> str:
    """
    Export data to text format
    """
    try:
        with io.StringIO() as buffer:
            for doc in data:
                summary = await get_each_doc_summary(client, doc['content'])
                title = await get_doc_title(client, doc['content'])
                doc['summary'] = summary
                doc['title'] = title
                for key, value in doc.items():
                    buffer.write(f"{key}: {value}\n")
                buffer.write("\n---\n\n")
            return buffer.getvalue()
    except Exception as e:
        logger.error(f"Error exporting to text: {e}")
        raise
    
async def save_file(file_path: Path, content):
    """
    Save content to a file asynchronously.
    """
    try:
        os.makedirs(file_path.parent, exist_ok=True)
        
        if file_path.suffix == ".json":
            async with aiofiles.open(file_path, mode="w", encoding="utf-8") as file:
                await file.write(json.dumps(content, indent=4, ensure_ascii=False))
        else:
            async with aiofiles.open(file_path, mode="w", encoding="utf-8") as file:
                await file.write(content)
    except Exception as e:
        logger.error(f"Error saving file: {e}")
        raise

async def main():
    keywords = [
        #"تصرف عدوانی",
        "سرقت",
        #"تغییر کاربری اراضی زراعی",
        #"اراضی ملی",
    ]
    hosts = [os.getenv("ELASTICSEARCH_HOST")]
    password = os.getenv("ELASTICSEARCH_PASSWORD")
    username = os.getenv("ELASTICSEARCH_USERNAME")
    client = AsyncOpenAI(base_url=os.getenv("OPENAI_BASE_URL"), api_key=os.getenv("OPENAI_API_KEY"))
    
    async with ElasticsearchService(hosts=hosts, password=password, username=username) as es:
        for keyword in keywords:
            logger.info(f"Processing keyword: {keyword}")
            data = await get_data(es, "edarehoquqy", keyword, max_results=10)
            if data:
                text_content = await export_to_text(data, client)
                output_dir = Path.home() / "edarehoquqy-output"
                await save_file(output_dir / f"{keyword}.txt", text_content)
                
                logger.info(f"Exported {len(data)} documents for '{keyword}'")
    
    logger.info(f"Total unique IDs found: {len(total_ids)}")

if __name__ == "__main__":
    asyncio.run(main())