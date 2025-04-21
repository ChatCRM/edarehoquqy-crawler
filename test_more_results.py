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

async def test_with_query(query, max_results=None):
    """Run a test with the specified query and max_results limit."""
    hosts = os.getenv("ELASTICSEARCH_HOST", "https://es.openairan.info")
    password = os.getenv("ELASTICSEARCH_PASSWORD")
    username = os.getenv("ELASTICSEARCH_USERNAME")
    
    logger.info(f"=========== Testing with query: '{query}' (max: {max_results}) ===========")
    
    es_service = ElasticsearchService(
        hosts=[hosts], 
        password=password, 
        username=username,
        timeout=60,
        retry_on_timeout=True,
        max_retries=3
    )
    
    # Try to connect and ensure index exists
    await es_service.ensure_index_exists()
    
    try:
        document_count = 0
        total_count = 0
        all_doc_ids = set()
        
        async for batch in es_service.search_by_text_batch(query, max_results=max_results):
            # Check if this is a batch with metadata
            if "total_count" in batch:
                total_count = batch["total_count"]
                results = batch["results"]
                logger.info(f"Total matching documents in Elasticsearch: {total_count}")
            else:
                results = batch["results"]
                
            # Process the results in this batch
            document_count += len(results)
            
            # Track document IDs to verify no duplicates
            for doc in results:
                doc_id = doc.get("id_edarehoquqy") or doc.get("id_ghavanin", "unknown")
                all_doc_ids.add(doc_id)
            
            logger.info(f"Retrieved {document_count} documents so far (batch size: {len(results)})")
            
            # If we've retrieved more than 20 documents, we've confirmed it works beyond 16
            if document_count > 20 and document_count % 20 == 0:
                logger.info(f"Successfully retrieved beyond 16 documents! Current count: {document_count}")
                
            # If we've reached 50, we can stop for testing purposes
            if max_results is None and document_count >= 50:
                logger.info("Reached 50 documents, stopping for testing efficiency")
                break
        
        logger.info(f"Test complete: Retrieved {document_count} unique documents out of {total_count} available")
        logger.info(f"Number of unique document IDs: {len(all_doc_ids)}")
    
    except Exception as e:
        logger.error(f"Error during test: {e}")
        logger.error(traceback.format_exc())
    finally:
        await es_service.close()

async def run_tests():
    """Run multiple tests with different queries."""
    logger.info("Starting Elasticsearch search tests...")
    
    # Common terms that should have lots of matches
    await test_with_query("قانون")  # Law - very common term
    
    # Test with a very short single word (should match many docs)
    await test_with_query("حق")  # Right - common term
    
    # Test with an empty search (match all) with limit
    await test_with_query("", max_results=30)  # Should match everything

if __name__ == "__main__":
    asyncio.run(run_tests()) 