#!/usr/bin/env python3
import asyncio
import traceback
import os
import logging
import sys
from dotenv import load_dotenv
from src.elasticsearch_service import ElasticsearchService

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def test_es_search():
    hosts = os.getenv("ELASTICSEARCH_HOST", "https://es.openairan.info")
    password = os.getenv("ELASTICSEARCH_PASSWORD")
    username = os.getenv("ELASTICSEARCH_USERNAME")
    
    query = "تعارض ثبتی"
    
    es_service = ElasticsearchService(
        hosts=[hosts], 
        password=password, 
        username=username,
        timeout=30,
        retry_on_timeout=True,
        max_retries=3
    )
    
    # Try to connect and ensure index exists
    await es_service.ensure_index_exists()
    
    try:
        document_count = 0
        total_count = 0
        
        logger.info(f"Searching for documents matching query: '{query}'")
        
        async for batch in es_service.search_by_text_batch(query):
            # Check if this is a batch with metadata
            if "total_count" in batch:
                total_count = batch["total_count"]
                results = batch["results"]
                logger.info(f"Total matching documents in Elasticsearch: {total_count}")
            else:
                results = batch["results"]
                
            # Process the results in this batch
            document_count += len(results)
            
            # Display some info about first document in each batch
            if results:
                first_doc = results[0]
                doc_id = first_doc.get("id_edarehoquqy") or first_doc.get("id_ghavanin", "unknown")
                logger.info(f"Batch contains document ID: {doc_id}")
            
            logger.info(f"Retrieved {document_count} documents so far")
        
        logger.info(f"Total documents retrieved: {document_count} out of {total_count} available")
    
    except Exception as e:
        logger.error(f"Error during test: {e}")
        logger.error(traceback.format_exc())
    finally:
        await es_service.close()
        logger.info("Test completed")

if __name__ == "__main__":
    asyncio.run(test_es_search()) 