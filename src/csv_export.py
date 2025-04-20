from elasticsearch import AsyncElasticsearch
import pandas as pd
import logging
import aiofiles
import asyncio
import os
from typing import List
from pathlib import Path
from dotenv import load_dotenv
from pydantic import BaseModel

from src.elasticsearch_service import ElasticsearchService

load_dotenv()

total_ids = set()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
# "_source": ["question", "answer", "content", "metadata", "id_ghavanin", "id_edarehoquqy"],
"""
        "metadata": {
          "properties": {
            "file_id": {
              "type": "keyword"
            },
            "file_number": {
              "type": "keyword",
              "null_value": "نامشخص"
            },
            "opinion_date": {
              "properties": {
                "gregorian": {
                  "type": "date",
                  "format": "yyyy/MM/dd",
                  "null_value": "0001/01/01"
                },
                "shamsi": {
                  "type": "keyword",
                  "null_value": "0001/01/01"
                }
              }
            },
            "opinion_number": {
              "type": "keyword",
              "null_value": "نامشخص"
            }
          }
        },
"""
class Metadata(BaseModel):
    file_id: str
    file_number: str
    opinion_date: dict
    opinion_number: str

class EdarehoquqyDataModel(BaseModel):
    id_edarehoquqy: str
    question: str
    answer: str
    content: str
    metadata: dict
    
    @property
    def metadata_obj(self):
        return Metadata(**self.metadata)


async def get_csv_data(es: ElasticsearchService, index_name: str, query: dict) -> pd.DataFrame:
    """
    Get data from Elasticsearch and return a pandas DataFrame.

    Args:
        es: AsyncElasticsearch client
        index_name: Name of the Elasticsearch index
        query: Query to search for

    Returns:
        pandas DataFrame containing the data
    """
    try:
        # Search for the data
        response = await es.search_by_text(query)

        if not response:
            logger.warning(f"No data found for query: {query}")
            return pd.DataFrame()

        
        for data in response:
            # Create a pandas DataFrame
            df = pd.DataFrame(data)
            
            # Check if id_edarehoquqy exists in the DataFrame
            if 'id_edarehoquqy' in df.columns:
                id_field = 'id_edarehoquqy'
                print(f"id_edarehoquqy: {df['id_edarehoquqy']['file_id']}")
                total_ids.add(df['id_edarehoquqy']['file_id'])
            elif 'id_ghavanin' in df.columns:
                id_field = 'id_ghavanin'
                print(f"id_ghavanin: {df['id_ghavanin']['file_id']}")
                total_ids.add(df['id_ghavanin']['file_id'])
            else:
                # If neither field exists, create a default ID
                logger.warning("Neither 'id_edarehoquqy' nor 'id_ghavanin' found in results")
                df['url'] = "https://edarehoquqy.eadl.ir/"
                return df
                
            # Use the appropriate ID field
            df['url'] = df[id_field].apply(lambda x: f"https://edarehoquqy.eadl.ir/%D8%AC%D8%B3%D8%AA%D8%AC%D9%88%DB%8C-%D9%86%D8%B8%D8%B1%DB%8C%D8%A7%D8%AA-%D9%85%D8%B4%D9%88%D8%B1%D8%AA%DB%8C/%D8%AC%D8%B3%D8%AA%D8%AC%D9%88%DB%8C-%D9%86%D8%B8%D8%B1%DB%8C%D9%87/moduleId/1286/controller/Search/action/Detail?IdeaId={x}")
            df.drop(columns=[id_field], inplace=True)
            return df
    except Exception as e:
        logger.error(f"Error getting data from Elasticsearch: {e}")
        # Print more detailed error information
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return pd.DataFrame()


def export_to_csv(df: pd.DataFrame) -> str:
    """
    Export a pandas DataFrame to a CSV file.

    Args:
        df: pandas DataFrame to export
        output_file: Path to the output CSV file
    """
    try:
        # Export the DataFrame to a CSV file
        return df.to_csv(index=False)
    except Exception as e:
        logger.error(f"Error exporting DataFrame to CSV: {e}")
        raise 
    
async def save_file(file_path: Path, content: str):
    """
    Save content to a file asynchronously.

    Args:
        file_path: Path to the file to save
        content: Content to save to the file
    """
    try:
        async with aiofiles.open(file_path, mode="w", encoding="utf-8") as file:
            await file.write(content)
            return None
    except Exception as e:
        logger.error(f"Error saving file: {e}")
        raise

async def save_obj_to_file(path: Path, obj: EdarehoquqyDataModel) -> None:
    """
    Save an object to a file asynchronously.
    """
    try:
        ...
    except Exception as e:
        logger.error(f"Error saving object to file: {e}")
        raise

async def save_batch_content(path: Path, content_list: List[EdarehoquqyDataModel]) -> None:
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
        "تصرف عدوانی",
        "سرقت",
        "تغییر کاربری اراضی زراعی",
        "اراضی ملی",
    ]
    hosts: list[str] = [os.getenv("ELASTICSEARCH_HOST")]
    password: str = os.getenv("ELASTICSEARCH_PASSWORD")
    username: str = os.getenv("ELASTICSEARCH_USERNAME")
    async with ElasticsearchService(hosts=hosts, password=password, username=username) as es:
        for keyword in keywords:
            df = await get_csv_data(es, "edarehoquqy", keyword)
            csv_content = export_to_csv(df)
            output_dir = Path.home() / "Documents" / "edarehoquqy-output"
            os.makedirs(output_dir, exist_ok=True)
            await save_file(output_dir / f"{keyword}.csv", csv_content)
        
    print(f"Total IDs length: {len(total_ids)}")

if __name__ == "__main__":
    asyncio.run(main())