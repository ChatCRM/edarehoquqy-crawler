#!/usr/bin/env python3
import argparse
import asyncio
import json
import logging
import sys
from pathlib import Path
import re
import os
from openai import AsyncOpenAI
from typing import Tuple, AsyncGenerator, List, Optional, Union, Dict
from pydantic import BaseModel
import aiofiles
from aiopath import AsyncPath


from src.csv_export import save_file, get_each_doc_summary, get_doc_title, EdarehoquqyDocument
from src.html_export import document_to_html, save_html_file
from src.elasticsearch_service import ElasticsearchService

QUERY_LIST_V1 = [
    "تصرف عدوانی",
    "خلع ید",
    "ممانعت از حق",
    "مزاحمت",
    "حق کسب و پیشه",
    "سرقفلی",
    "تقسیم مال مشاعی",
    "افراز",
    "فروش سهام",
    "ابطال سند",
    "بطلان معامله",
    "اثبات مالکیت",
    "تعارض ثبتی",
    "سرقت",
    "کلاهبرداری",
    "اخذ به شفعه",
    "اشتباه در معامله",
    "صحت قرارداد",
    "الزام به تنظیم سند رسمی",
    "تحویل مبیع",
    "مطالبه وجه چک",
    "استرداد لاشه چک",
    "خیانت در امانت",
    "اختلاس",
    "پولشویی",
    "انحلال شرکت",
    "ایفای تعهدات",
    "تغییر کاربری اراضی زراعی",
    "اراضی ملی",
    "اراضی موات",
    "ماده ۱۰۰ شهرداری",
    "کمیسیون ماده ۵۵ شهرداری",
    "تعیین وضعیت املاک واقع در طرحهای عمرانی",
    "ابطال پروانه ساختمان",
    "تغییر کاربری",
    "الزام به صدور پروانه ساختمانی",
    "شرط انفساخ قرارداد",
    "خیار فسخ",
    "خیار تدلیس",
    "خیار غبن",
    "خیار عیب",
    "خیار تخلف از شرط",
    "اقاله قرارداد",
    "قرارداد محرمانگی",
    "قرارداد منع افشای اطلاعات",
    "اسرار تجاری",
    "اسرار حرفه ای",
    "اطلاعات سری",
    "ابطال اجرائیه",
    "اجرای ثبت",
    "مطالبه مهریه",
    "طلاق توافقی",
    "طلاق به درخواست زوجه",
    "طلاق رجعی",
    "ترک انفاق",
    "نفقه زوجه",
    "اجرت‌المثل ایام زوجیت",
    "اجرت‌المثل",
    "دستور تخلیه",
    "جعل سند رسمی",
    "جعل سند عادی",
    "حق انتفاع",
    "حق ارتفاق",
    "وقف",
    "ودیعه",
    "قرض",
    "هبه",
    "صلح",
    "ضمانت",
    "ابطال ضمانت نامه بانکی",
    "جعاله",
    "غصب",
    "اتلاف",
    "تسبیب",
    "مسئولیت مدنی",
    "بیمه شخص ثالث",
    "بیمه حوادث",
    "مسئولیت قراردادی",
    "ارث",
    "وصیت",
    "حجر",
    "اثبات حجر",
    "اهلیت",
    "تابعیت",
    "اقامتگاه",
    "حضانت",
    "نفی نسب",
    "اثبات نسب",
    "ولایت قهری",
    "اقرار",
    "شهادت",
    "قسم",
    "ادله اثبات دعوی",
    "حق حبس",
    "تاخیر تأدیه",
    "اجاره به شرط تملیک",
    "إبراء",
    "تهاتر",
    "ایفای تعهد",
    "اختراع",
    "کپی رایت",
    "علامت تجاری",
    "نام تجاری",
    "قاعده لاضرر",
    "قاعده ید",
    "اماره",
    "استصحاب",
    "برائت",
    "قاعده درء",
    "منع تعقیب",
    "موقوفی تعقیب",
    "عدم النفع",
    "قتل عمد",
    "قتل شبه عمد",
    "محاربه",
    "افساد فی الارض",
    "لواط",
    "شرب خمر",
    "قذف",
    "ارتداد",
    "مواد مخدر",
    "ارتشاء",
    "هک",
    "کلاهبرداری رایانه‌ای",
    "انتشار اطلاعات محرمانه",
    "توهین به مقدسات",
    "افتراء",
    "جرائم علیه امنیت ملی",
    "آدم ربایی",
    "تخریب",
    "الزام به تمکین",
    "فسخ نکاح",
    "نشر اکاذیب",
    "توهین",
    "اشاعه فحشا",
    "شهادت کذب",
    "جرائم مطبوعاتی",
    "تعدی و تفریط",
    "استفاده از سند مجعول",
    "قاچاق مواد مخدر",
    "کمیسیون گمرکی",
    "فرار مالیاتی",
    "مالیات بر درآمد",
    "مالیات بر ارث",
    "اهانت به مقدسات",
    "حمل غیر مجاز اسلحه",
    "خطای پزشکی",
    "صلاحیت ذاتی",
    "صلاحیت محلی",
    "واخواهی",
    "اعاده دادرسی",
    "دعوای تقابل",
    "افزایش خواسته",
    "کاهش خواسته",
    "عدم استماع دعوا",
    "رد دعوا",
    "صلح و سازش",
    "کارشناسی",
    "دستور موقت",
    "تامین خواسته",
    "توقیف اموال",
    "ابطال مزایده",
    "اعسار از پرداخت هزینه دادرسی",
    "اعسار از پرداخت محکوم به",
    "هزینه دادرسی",
    "اعتبار امر مختومه",
    "مصادره اموال",
    "اصل ۴۹ قانون اساسی",
    "تجدیدنظر خواهی",
    "فرجام خواهی",
    "ادله الکترونیکی",
    "ضبط وثیقه",
    "کفالت",
    "تبدیل تعهد",
    "شکایت انتظامی",
    "تخلفات انتظامی قضات",
    "مسئولیت مدیران",
    "شرکت با مسئولیت محدود",
    "شرکت تضامنی",
    "حق العمل کاری",
    "ضمانت نامه بانکی",
    "اعتبار اسنادی",
    "ورشکستگی",
    "دلالی",
    "قرارداد نمایندگی",
    "هیأت مدیره",
    "سهام ممتاز",
    "سهام عادی",
    "افزایش سرمایه",
    "ادغام",
    "کاهش سرمایه",
    "انحلال شرکت",
    "سفته",
    "برات",
    "اوراق قرضه",
    "مشارکت مدنی",
    "ایفای تعهدات ارزی",
    "مضاربه",
    "ابطال سود مازاد بانکی"
]

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class QueryKeywords(BaseModel):
    keywords: List[str]

async def get_query_keywords(client: AsyncOpenAI, query: str, max_keywords: int = 12) -> List[str]:
    """
    Generate up to a maximum number of relevant search keywords for an Iranian legal query.
    
    Args:
        client: AsyncOpenAI client
        query: The main legal query or topic (can be in Persian or English)
        max_keywords: Maximum number of keywords to generate (default: 12)
        
    Returns:
        List of relevant search keywords (maximum of max_keywords)
    """
    # Prompt in both Persian and English to handle both types of queries
    prompt = f"""
    تولید کلمات کلیدی برای جستجوی حقوقی ایرانی: "{query}"

    هدف: ایجاد حداکثر {max_keywords} کلمه کلیدی برای جستجوی مؤثر در پایگاه داده حقوقی ایران در مورد موضوع "{query}".

    دستورالعمل‌ها:
    1. اصطلاحات رسمی حقوقی مرتبط با موضوع را در قوانین ایران شناسایی کنید
    2. شامل اصطلاحات متداول و روزمره که افراد غیر حقوقی ممکن است استفاده کنند
    3. مواد قانونی مرتبط، شماره‌های ماده و ارجاعات قانونی خاص را در نظر بگیرید
    4. اصطلاحات مربوط به رویه‌های قضایی و دادرسی مرتبط را شامل کنید
    5. از منظر خواهان و خوانده به موضوع نگاه کنید
    6. مترادف‌ها و واریاسیون‌های اصطلاحات کلیدی را شامل کنید
    7. اصطلاحات مربوط به نتایج، مجازات‌ها یا راه‌حل‌های قانونی را در نظر بگیرید
    8. عبارات ترکیبی که احتمالاً در متن قوانین یا آرای قضایی وجود دارند
    9. از کلمات کلیدی مرتبط با رویه قضایی فعلی دادگاه‌های ایران استفاده کنید
    10. ترکیبی از کلمات تک و عبارات کوتاه را در نظر بگیرید

    فقط لیستی از کلمات کلیدی به زبان فارسی (حداکثر {max_keywords} مورد) را برگردانید که برای بازیابی اسناد مرتبط با "{query}" در یک پایگاه داده حقوقی ایرانی مؤثرترین باشند. فقط کلمات کلیدی را برگردانید، بدون توضیح اضافی.

    ---

    ENGLISH INSTRUCTIONS (FOR BETTER UNDSTANDING OF THE QUERY BUT THE OUTPUT NEEDS TO BE IN PERSIAN!):
    Generate keyword list for Iranian legal search: "{query}"

    Objective: Create up to {max_keywords} keywords for effective searching in an Iranian legal database regarding "{query}".

    Guidelines:
    1. Identify formal legal terms related to the topic in Iranian law (in Persian)
    2. Include common terms that non-legal people might use
    3. Consider relevant legal articles, article numbers, and specific legal references
    4. Include terms related to judicial procedures and relevant proceedings
    5. Look at the issue from both plaintiff and defendant perspectives
    6. Include synonyms and variations of key terms
    7. Consider terms related to legal outcomes, penalties, or remedies
    8. Include compound phrases likely to appear in legal texts or judicial opinions
    9. Use keywords related to current judicial practice in Iranian courts
    10. Use a mix of single words and short phrases

    Return ONLY a list of Persian keywords (maximum {max_keywords}) that would be most effective for retrieving documents related to "{query}" in an Iranian legal database. Return only the keywords without additional explanation.
    """
    
    logger.info(f"Generating keywords for query: {query}")
    try:
        response = await client.beta.chat.completions.parse(
            model="gpt-4.1-mini-2025-04-14", 
            response_format=QueryKeywords,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=1500,
            temperature=0.1
        )
        
        keywords = response.choices[0].message.parsed.keywords
        logger.info(f"output: {response.choices[0].message.parsed}")
        
        # Ensure we don't exceed max_keywords
        result = keywords[:max_keywords]
        logger.info(f"Generated {len(result)} keywords for '{query}'")
        return result
    except Exception as e:
        logger.error(f"Error generating keywords for '{query}': {e}")
        # Fallback to basic keywords if there's an error
        if isinstance(query, str) and query.strip():
            return [query]
        return []

def transform_query(query: str) -> List[str]:
    """
    Split a hyphen‑separated list of phrases into a List[str].
    E.g. "foo - bar baz- qux" -> ["foo", "bar baz", "qux"]
    
    Args:
        query: A string with phrases separated by hyphens, or a list
        
    Returns:
        List of non-empty phrases
    """
    if isinstance(query, list):
        return [p.strip() for p in query if p and p.strip()]
        
    # Handle string input
    logger.info(f"Transforming query: {query}")
    if not query or not isinstance(query, str):
        logger.warning(f"Invalid query type or empty query: {type(query)}")
        return []
        
    # Split by hyphens
    parts = re.split(r"\s*-\s*", query)
    
    # Filter out empty parts and strip whitespace
    result = [p.strip() for p in parts if p and p.strip()]
    
    logger.info(f"Transformed to {len(result)} phrases: {result}")
    return result


async def check_if_document_exists(output_dir: str, id_edarehoquqy: str) -> Union[bool, EdarehoquqyDocument]:
    """
    Check if a document with the given ID exists in the output directory.
    
    Args:
        output_dir: Path to the output directory
        id_edarehoquqy: ID of the document to check
        
    Returns:
        True if the document exists, False otherwise
    """
    file_path = AsyncPath(output_dir) / id_edarehoquqy
    if not file_path.exists():
        return False
    json_file = file_path / f"{id_edarehoquqy}.json"
    if not json_file.exists():
        return False
    async with aiofiles.open(json_file, mode='r', encoding='utf-8') as f:
        data = await f.read()
    return json.loads(data, object_hook=lambda d: EdarehoquqyDocument(**d))

async def get_all_existing_ids(output_dir: str) -> Dict[str, AsyncPath]:
    """
    Get all existing document IDs from the output directory.
    
    Args:
        output_dir: Path to the output directory
        
    Returns:
        Dict of existing document IDs and their corresponding file paths
    """
    existing_ids = {}
    output_path = AsyncPath(output_dir)
    
    if not await output_path.exists():
        return existing_ids
        
    async for file_path in output_path.glob('**/*.json'):
        try:
            id_edarehoquqy = file_path.stem
            keyword = file_path.parent.name
            dir_path = AsyncPath(output_dir) / keyword / id_edarehoquqy
            if await dir_path.exists():
                existing_ids[id_edarehoquqy] = dir_path
        except Exception as e:
            logger.error(f"Error processing file {file_path}: {e}")
    
    return existing_ids



async def get_data(query_list: List[str], client: AsyncOpenAI, output_dir: str = "../html-output") -> AsyncGenerator[Tuple[EdarehoquqyDocument, str], None]:
    """
    Retrieve documents from Elasticsearch based on query list.
    
    Args:
        query_list: List of queries to search for
        client: Optional OpenAI client for generating titles and summaries
        output_dir: Directory to save output files
        
    Yields:
        EdarehoquqyDocument objects with data from Elasticsearch + Query in a Tuple in the same order
    """
    
    es_service = None
    
    try:
        hosts = os.getenv("ELASTICSEARCH_HOST")
        password = os.getenv("ELASTICSEARCH_PASSWORD")
        username = os.getenv("ELASTICSEARCH_USERNAME")

        processed_files = set()
        existing_ids = await get_all_existing_ids(output_dir)
        # Create ES service
        es_service = ElasticsearchService(
            hosts=[hosts], 
            password=password, 
            username=username,
            timeout=120,  # Increased timeout for slower connections
            retry_on_timeout=True,
            max_retries=5  # Increased retry count
        )
        
        # Try to connect and ensure index exists
        await es_service.ensure_index_exists()
        
        for query_text in query_list:
            logger.info(f"Searching for documents matching query: '{query_text}'")
            
            try:
                document_count = 0
                # Set parameters based on the number of phrases
                query_phrases = await get_query_keywords(client, query_text)
                num_phrases = len(query_phrases) if isinstance(query_phrases, list) else 1
                
                min_score = 1.5  # Base value
                if num_phrases > 5:
                    min_score = 2.0
                elif num_phrases > 2:
                    min_score = 2.5
                
                
                max_results = None
                logger.info(f"Searching with {num_phrases} phrases, min_score={min_score}, max_results={max_results}")
                
                # Execute search with dynamic parameters
                async for batch in es_service.search_by_text_batch(
                    query_phrases, 
                    min_score=min_score, 
                    max_results=max_results
                ):
                    if batch is None:
                        continue
                        
                    # Extract the results list from the batch
                    results = batch.get("results", [])
                    relevant_count = batch.get("relevant_count", len(results))
                    total_count = batch.get("total_count", 0)
                    precision = batch.get("precision", 0)
                    
                    logger.info(f"Processing batch with {len(results)} results (relevant: {relevant_count} of {total_count} total, precision: {precision:.1%})")
                    
                    for source in results:
                        document_count += 1
                        doc_id = source.get('id_edarehoquqy')
                        if not doc_id:
                            logger.warning(f"Missing id_edarehoquqy in document: {source.keys()}")
                            continue
                            
                        if doc_id in existing_ids:
                            logger.info(f"Already processed file with ID: {doc_id}")
                            doc_path = existing_ids[doc_id] / doc_id
                            if doc := await check_if_document_exists(doc_path, doc_id):
                                logger.info(f"Loaded document from {doc_path}")
                                yield doc, query_text
                                continue

                        doc = EdarehoquqyDocument(
                            id_edarehoquqy=doc_id,
                            question=source.get('question', ''),
                            answer=source.get('answer', ''),
                            content=source.get('content', ''),
                            title='',  # Will be populated later if needed
                            summary='',  # Will be populated later if needed
                            metadata=source.get('metadata', {})
                        )
                        
                        # Generate title
                        doc_content = f"Question: {doc.question}\nAnswer: {doc.answer}"
                        doc.title = await get_doc_title(client, doc_content)
                        # Generate summary
                        doc.summary = await get_each_doc_summary(client, doc_content)
                        logger.info(f"Generated title and summary for document {doc.id_edarehoquqy}")
                        processed_files.add(doc.id_edarehoquqy)
                        
                        yield doc, query_text
                        
                logger.info(f"Processed {document_count} documents for query: '{query_text}'")
                if document_count == 0:
                    logger.warning(f"No documents found for query: '{query_text}'. Try lowering min_score or adjusting the query.")
                    
            except Exception as e:
                logger.error(f"Error searching for query '{query_text}': {e}")
                continue
    except Exception as e:
        logger.error(f"Error connecting to Elasticsearch: {e}")
    finally:
        # Ensure resources are cleaned up
        if es_service:
            try:
                await es_service.close()
                logger.info("Closed Elasticsearch connection")
            except Exception as e:
                logger.error(f"Error closing Elasticsearch connection: {e}")

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
            metadata=data.get('metadata', {})
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
    
    # Get IranSans font path
    font_path = Path(__file__).parent.parent / "assets" / "fonts" / "IRANSans.woff2"
    if not font_path.exists():
        logger.warning(f"IranSans font not found at {font_path}, HTML will use fallback fonts")
        font_path = None
    else:
        logger.info(f"Using IranSans font from {font_path}")
    
    saved_files = []
    document_counter = 0
    
    logger.info(f"Converting and saving documents for queries: {query_list}")
    
    try:
        # Process each document
        async for doc_data, query in get_data(query_list, client, str(output_dir)):
            document_counter += 1
            
            try:
                # Create query-specific subdirectory
                query_dir = output_dir / query.replace('/', '_').replace(' ', '_') / doc_data.id_edarehoquqy.replace('/', '_')
                os.makedirs(query_dir, exist_ok=True)
                
                # Save document as HTML with embedded font
                html_path = await save_html_file(doc_data, query_dir, font_path=font_path)
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
    except Exception as e:
        logger.error(f"Error retrieving documents from Elasticsearch: {e}")
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
    
    # Get IranSans font path
    font_path = Path(__file__).parent.parent / "assets" / "fonts" / "IRANSans.woff2"
    if not font_path.exists():
        logger.warning(f"IranSans font not found at {font_path}, HTML will use fallback fonts")
        font_path = None
    else:
        logger.info(f"Using IranSans font from {font_path}")
    
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
            
            # Save as HTML with embedded font
            html_path = await save_html_file(doc, file_dir, font_path=font_path)
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
    parser.add_argument("-q", "--queries", nargs="*", default=None, help="Search queries to retrieve documents from Elasticsearch (optional)")
    parser.add_argument("-f", "--files", nargs="*", help="JSON files to convert to HTML")
    parser.add_argument("-o", "--output-dir", help="Output directory (default: ./html-output)",
                       default="./html-output")
    parser.add_argument("-k", "--generate-keywords", action="store_true", 
                        default=True, help="Generate keywords for each query (uses OpenAI API)")
    
    args = parser.parse_args()
    
    output_dir = Path(args.output_dir)
    
    # Initialize OpenAI client if generating content
    logger.info("Initializing OpenAI client for content generation")
    if os.getenv("OPENAI_API_KEY") is None or os.getenv("OPENAI_BASE_URL") is None:
        logger.error("OPENAI_API_KEY or OPENAI_BASE_URL is not set. Please set it in your environment variables.")
        sys.exit(1)
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
        
        # Process elasticsearch queries if provided or if no flags are specified
        if args.queries is not None or (not args.files and args.queries is None):
            if not ElasticsearchService:
                logger.error("ElasticsearchService not available. Cannot query Elasticsearch.")
                logger.error("Please make sure the elasticsearch package is installed.")
                sys.exit(1)

            # Check for required environment variables
            es_env_vars = ["ELASTICSEARCH_HOST", "ELASTICSEARCH_USERNAME", "ELASTICSEARCH_PASSWORD"]
            missing_vars = [var for var in es_env_vars if not os.getenv(var)]
            if missing_vars:
                logger.error(f"Missing required environment variables: {', '.join(missing_vars)}")
                logger.error("Please set these environment variables or use the --files option instead.")
                sys.exit(1)

            es_output_dir = output_dir / "search"
            try:
                logger.info(f"Searching with queries: {QUERY_LIST_V1}")
                es_results = await convert_and_save_documents(QUERY_LIST_V1, es_output_dir, client)
                saved_files.extend(es_results)
            except Exception as e:
                logger.error(f"Error retrieving documents from Elasticsearch: {e}")
                logger.error("If you're having connection issues, try using the --files option instead")
                import traceback
                logger.error(traceback.format_exc())
        
        # Print results
        logger.info(f"Successfully saved {len(saved_files)} HTML files")
        if saved_files:
            logger.info(f"Files saved to: {output_dir}")
    except Exception as e:
        logger.error(f"Error during conversion: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)
    finally:
        # Clean up any remaining client sessions
        if 'asyncio' in sys.modules:
            for task in asyncio.all_tasks():
                if not task.done() and task != asyncio.current_task():
                    try:
                        task.cancel()
                    except Exception:
                        pass

if __name__ == "__main__":
    asyncio.run(main()) 