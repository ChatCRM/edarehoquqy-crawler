#!/usr/bin/env python3
import argparse
import asyncio
import json
import logging
import sys
from pathlib import Path
import os
from openai import AsyncOpenAI
from typing import Tuple, AsyncGenerator, List, Optional, Union, Dict
import aiofiles
from aiopath import AsyncPath
from src.csv_export import save_file, get_each_doc_summary, get_doc_title, EdarehoquqyDocument
from src.html_export import document_to_html, save_html_file
from src.elasticsearch_service import ElasticsearchService

QUERY_LIST = [
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

async def get_all_existing_ids(output_dir: str) -> Dict[str, EdarehoquqyDocument]:
    """
    Get all existing document IDs from the output directory.
    
    Args:
        output_dir: Path to the output directory
        
    Returns:
        Dict of existing document IDs and their corresponding EdarehoquqyDocument objects
    """
    existing_ids = {}
    for file_path in AsyncPath(output_dir).glob('**/*.json'):
        id_edarehoquqy = file_path.stem
        keyword = file_path.parent.name
        file_path = AsyncPath(output_dir) / keyword / id_edarehoquqy
        if await file_path.exists():
            existing_ids[id_edarehoquqy] = file_path
    return existing_ids




async def get_data(query_list: List[str], client: AsyncOpenAI, output_dir: str = "../html-output") -> AsyncGenerator[Tuple[EdarehoquqyDocument, str], None]:
    """
    Retrieve documents from Elasticsearch based on query list.
    
    Args:
        query_list: List of queries to search for
        client: Optional OpenAI client for generating titles and summaries
        max_results: Maximum number of results to return per query. If None, returns all matching documents.
        
    Yields:
        EdarehoquqyDocument objects with data from Elasticsearch + Query in a Tuple in the same order
    """
    if ElasticsearchService is None:
        logger.error("ElasticsearchService not available. Cannot retrieve documents from Elasticsearch.")
        return
        
    hosts = os.getenv("ELASTICSEARCH_HOST")
    password = os.getenv("ELASTICSEARCH_PASSWORD")
    username = os.getenv("ELASTICSEARCH_USERNAME")

    processed_files = set()
    existing_ids = await get_all_existing_ids(output_dir)
    
    es_service = None
    try:
        # Create ES service
        es_service = ElasticsearchService(
            hosts=[hosts], 
            password=password, 
            username=username,
            timeout=30,  # Reduced timeout for faster failure
            retry_on_timeout=True,
            max_retries=3
        )
        
        # Try to connect and ensure index exists
        await es_service.ensure_index_exists()
        
        for query in query_list:
            logger.info(f"Searching for documents matching query: '{query}'")
            
            try:
                document_count = 0
                async for batch in es_service.search_by_text_batch(query):
                    if batch is None:
                        continue
                        
                    # Extract the results list from the batch
                    # Batches are dictionaries with a "results" key or a "total_count" and "results" keys
                    results = batch.get("results", [])
                    
                    for source in results:
                        document_count += 1
                        doc_id = source.get('id_edarehoquqy')
                        if doc_id in existing_ids:
                            logger.info(f"Already processed file with ID: {doc_id}")
                            doc_path = existing_ids[doc_id] / doc_id
                            if doc := await check_if_document_exists(doc_path, doc_id):
                                logger.info(f"Loaded document from {doc_path}")
                                yield doc, query
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
                        
                        yield doc, query
                logger.info(f"Processed {document_count} documents for query: '{query}'")
            except Exception as e:
                logger.error(f"Error searching for query '{query}': {e}")
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
        max_results: Maximum number of results to return per query. If None, returns all matching documents.
        
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
    
    # Process each document
    async for doc_data, query in get_data(query_list, client):
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

            es_output_dir = output_dir / "search"
            try:
                query_list = args.queries if args.queries is not None else QUERY_LIST
                es_results = await convert_and_save_documents(query_list, es_output_dir, client)
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
    finally:
        # Clean up any remaining client sessions
        if 'asyncio' in sys.modules:
            for task in asyncio.all_tasks():
                if not task.done() and task != asyncio.current_task():
                    task.cancel()

if __name__ == "__main__":
    asyncio.run(main()) 