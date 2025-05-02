from elasticsearch import AsyncElasticsearch
import logging
import aiofiles
import asyncio
import os
from typing import List, Dict, Set
from pathlib import Path
from dotenv import load_dotenv
from pydantic import Field, BaseModel
import pandas as pd
from tenacity import retry, stop_after_attempt, wait_exponential
from openai import AsyncOpenAI
import json
import io
import csv

from src.elasticsearch_service import ElasticsearchService

load_dotenv()


total_ids = set()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
# "_source": ["question", "answer", "content", "metadata", "id_ghavanin", "id_edarehoquqy"],
"""
{
    "id_edarehoquqy": "SAMPLE123",
    "question": "آیا سرقت اموال عمومی مشمول مجازات های شدیدتری نسبت به سرقت اموال خصوصی است؟",
    "answer": "بله، مطابق قانون مجازات اسلامی، سرقت اموال عمومی و دولتی مشمول مجازات های شدیدتری است. ماده ٦٥٤ قانون مجازات اسلامی تصریح می‌کند که هرگاه سرقت از اموال عمومی یا دولتی واقع شود و میزان آن بیش از حد نصاب مقرر باشد، مجازات مرتکب تشدید می‌گردد. همچنین بر اساس ماده ٥ قانون تشدید مجازات مرتکبین ارتشا، اختلاس و کلاهبرداری، در صورتی که سرقت همراه با تبانی صورت گیرد یا توسط مأمورین دولتی انجام شود، مجازات آن مضاعف خواهد بود. در این موارد، علاوه بر رد مال به صاحب آن و جبران خسارت وارده، مرتکب به حبس، جزای نقدی و انفصال از خدمات دولتی محکوم می‌شود.",
    "content": "آیا سرقت اموال عمومی مشمول مجازات های شدیدتری نسبت به سرقت اموال خصوصی است؟\n\nبله، مطابق قانون مجازات اسلامی، سرقت اموال عمومی و دولتی مشمول مجازات های شدیدتری است. ماده ٦٥٤ قانون مجازات اسلامی تصریح می‌کند که هرگاه سرقت از اموال عمومی یا دولتی واقع شود و میزان آن بیش از حد نصاب مقرر باشد، مجازات مرتکب تشدید می‌گردد. همچنین بر اساس ماده ٥ قانون تشدید مجازات مرتکبین ارتشا، اختلاس و کلاهبرداری، در صورتی که سرقت همراه با تبانی صورت گیرد یا توسط مأمورین دولتی انجام شود، مجازات آن مضاعف خواهد بود. در این موارد، علاوه بر رد مال به صاحب آن و جبران خسارت وارده، مرتکب به حبس، جزای نقدی و انفصال از خدمات دولتی محکوم می‌شود.",
    "summary": "سرقت اموال عمومی طبق قانون مجازات اسلامی و قانون تشدید مجازات مرتکبین ارتشا، اختلاس و کلاهبرداری، مشمول مجازات های شدیدتری نسبت به سرقت اموال خصوصی است، به ویژه در صورت تبانی یا انجام توسط مأمورین دولتی.",
    "title": "مجازات های شدیدتر برای سرقت اموال عمومی نسبت به اموال خصوصی",
    "metadata": {
        "file_id": "SAMPLE-FILE-001",
        "file_number": "123/456",
        "opinion_date": {
            "shamsi": "1402/05/12",
            "gregorian": "2023/08/03"
        },
        "opinion_number": "789/ABC"
    }
} 
"""
class Metadata(BaseModel):
    file_id: str
    file_number: str
    opinion_date: dict
    opinion_number: str

class EdarehoquqyDocument(BaseModel):
    id_edarehoquqy: str
    question: str
    answer: str
    content: str
    summary: str = Field(default="")
    title: str = Field(default="")
    metadata: dict = Field(default={})
    
    @property
    def metadata_obj(self):
        # Create a default metadata with required fields if empty or missing
        default_metadata = {
            "file_id": "default_id",
            "file_number": "default_number",
            "opinion_date": {"shamsi": "", "gregorian": ""},
            "opinion_number": "default_opinion_number"
        }
        
        # Use the defaults for any missing fields
        metadata_with_defaults = {**default_metadata, **self.metadata}
        return Metadata(**metadata_with_defaults)


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=15))
async def get_doc_title(client: AsyncOpenAI, doc: str) -> str:
    """
    Get data from Elasticsearch and return a pandas DataFrame.

    Args:
        client: AsyncOpenAI client instance
        doc: The document text to generate a title for
        
    Returns:
        A string containing the generated title
    """
    if not client:
        raise ValueError("Client is not initialized")
    
    prompt = f"""
    Generate a precise, searchable Persian title (10-12 words) for this Iranian legal document in Q&A format.
    
    The title should:
    - Be in Persian
    - Extract key legal terms/concepts directly from the question and answer
    - Include the main legal issue, parties involved, and legal principle addressed
    - Capture both what is being asked and what is concluded in the answer
    - Use precise Iranian legal terminology that matches terms in the document
    - Include relevant statute numbers, laws, or legal principles if mentioned
    - Be specific enough to distinguish this document from similar legal questions
    - Be searchable - include keywords someone would use to find this document
    - Avoid generic descriptions or unnecessary words
    - Be formatted as JSON with only a "title" field
    
    Document:
    {doc}
    """

    try:
        response = await client.chat.completions.create(
            model="gpt-4.1-mini-2025-04-14",  # Using the more capable model for better title generation
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"},
            temperature=0.1  # Lower temperature for more focused, precise titles
        )
        # Parse the JSON response and handle potential errors
        content = response.choices[0].message.content
        parsed_content = json.loads(content)
        return parsed_content.get("title", "")
    except json.JSONDecodeError as e:
        logger.error(f"JSON parsing error: {e}, Content: {response.choices[0].message.content}")
        raise
    except Exception as e:
        logger.error(f"Error generating title: {e}")
        raise

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=15))
async def get_each_doc_summary(client: AsyncOpenAI, doc: str) -> str:
    """
    Get a summary for a document and return it as a string.
    
    Args:
        client: AsyncOpenAI client instance
        doc: The document text to summarize
        
    Returns:
        A string containing the generated summary
    """
    prompt = f"""
    You are a helpful assistant that summarizes documents.
    Documents provided are about Iranian Documents and law disputes and discussions.
    The output of your summary should be in Persian and will be read by Lawyers and Experts in the field.
    Education level of the audience is Master and PhD and your summary should be in a way that is easy to understand for them and capture the main points.
    The summary should be in 150 words or less and must summarize the document in a way that is easy to understand for the audience.
    Output should be in JSON format with only a "summary" field.
    
    Please summarize the following document:
    {doc}
    """
    try:
        response = await client.chat.completions.create(
            model="gpt-4.1-mini-2025-04-14",
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"},
            max_tokens=250,
            temperature=0.3
        )
        
        # Parse the JSON response and handle potential errors
        content = response.choices[0].message.content
        parsed_content = json.loads(content)
        return parsed_content.get("summary", "")
    except json.JSONDecodeError as e:
        logger.error(f"JSON parsing error: {e}, Content: {response.choices[0].message.content}")
        raise
    except Exception as e:
        logger.error(f"Error generating summary: {e}")
        raise

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

async def save_obj_to_file(path: Path, obj: EdarehoquqyDocument) -> None:
    """
    Save an object to a file asynchronously.
    """
    try:
        ...
    except Exception as e:
        logger.error(f"Error saving object to file: {e}")
        raise

async def save_batch_content(path: Path, content_list: List[EdarehoquqyDocument]) -> None:
    """
    Save a list of content to files asynchronously.
    """
    path.mkdir(parents=True, exist_ok=True)
    for content in content_list:
        await save_obj_to_file(path / f"{content.metadata_obj.file_id}.md", content)
    return None

async def main():
    # Initialize the Elasticsearch client
    """
    keyword list:
    ۱) تصرف عدوانی
    ۲)خلع ید
    ۳) ممانعت از حق
    ۴)مزاحمت
    ۵)حق کسب و پیشه
    ۶) سرقفلی
    ۷) تقسیم مال مشاعی
    ۸) افراز 
    ۹) فروش سهام 
    ۱۰) ابطال سند
    ۱۱) بطلان معامله 
    ۱۲) اثبات مالکیت 
    ۱۳) تعارض ثبتی
    ۱۴) سرقت 
    ۱۵)کلاهبرداری
    ۱۶) اخذ به شفعه 
    ۱۷) اشتباه در معامله
    ۱۸)صحت قرارداد
    ۱۹) الزام به تنظیم سند رسمی 
    ۲۰) تحویل مبیع
    ۲۱) مطالبه وجه چک 
    ۲۲) استرداد لاشه چک
    ۲۳)خیانت در امانت
    ۲۴) اختلاس
    ۲۵) پولشویی 
    ۲۶) انحلال شرکت
    ۲۷) ایفای تعهدات 
    ۲۸) تغییر کاربری اراضی زراعی
    ۲۹) اراضی ملی
    """
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
                # Text export
                text_content = await export_to_text(data, client)
                output_dir = Path.home() / "edarehoquqy-output"
                await save_file(output_dir / f"{keyword}.txt", text_content)
                
                # HTML export (if imported)
                try:
                    from src.html_export import export_documents_to_html
                    
                    # Convert raw data to EdarehoquqyDocument objects
                    documents = []
                    for doc_data in data:
                        # Convert each document to proper object format
                        doc_obj = EdarehoquqyDocument(
                            id_edarehoquqy=doc_data.get('id_edarehoquqy', ''),
                            question=doc_data.get('question', ''),
                            answer=doc_data.get('answer', ''),
                            summary=doc_data.get('summary', ''),
                            title=doc_data.get('title', ''),
                            metadata=doc_data.get('metadata', {})
                        )
                        documents.append(doc_obj)
                    
                    # Export to HTML files
                    html_output_dir = Path.home() / "edarehoquqy-html-output" / keyword
                    await export_documents_to_html(documents, html_output_dir)
                    logger.info(f"Exported HTML files for '{keyword}'")
                except ImportError:
                    logger.info("HTML export module not available, skipping HTML export")
                except Exception as e:
                    logger.error(f"Error during HTML export: {e}")
                
                logger.info(f"Exported {len(data)} documents for '{keyword}'")
    
    logger.info(f"Total unique IDs found: {len(total_ids)}")

if __name__ == "__main__":
    asyncio.run(main())