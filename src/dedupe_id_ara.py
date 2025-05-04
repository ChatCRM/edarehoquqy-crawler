#!/usr/bin/env python3
"""
dedupe_id_ara.py

Find and delete all but one document per id_ara in the 'ara' index.
Uses environment variables for Elasticsearch credentials.
Uses asyncio for better performance.
"""

import os
import sys
import logging
import asyncio
import time
from elasticsearch import AsyncElasticsearch
from dotenv import load_dotenv

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("dedupe_ara.log")
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# ————— CONFIG —————
ES_HOST = os.getenv("ELASTICSEARCH_HOST", "http://localhost:9200")
ES_USERNAME = os.getenv("ELASTICSEARCH_USERNAME")
ES_PASSWORD = os.getenv("ELASTICSEARCH_PASSWORD")
INDEX = "ara"
DRY_RUN = os.getenv("DRY_RUN", "False").lower() in ("true", "1", "t")  # Set to True to preview without deleting
MAX_CONCURRENT = int(os.getenv("MAX_CONCURRENT", "20"))  # Maximum concurrent tasks
# ——————————————————

async def connect_elasticsearch():
    """
    Connect to Elasticsearch using credentials from environment variables
    """
    try:
        if ES_USERNAME and ES_PASSWORD:
            es = AsyncElasticsearch(
                ES_HOST,
                basic_auth=(ES_USERNAME, ES_PASSWORD),
                verify_certs=False,
                ssl_show_warn=False
            )
        else:
            es = AsyncElasticsearch(ES_HOST)
        
        if not await es.ping():
            logger.error("Failed to connect to Elasticsearch")
            sys.exit(1)
        
        logger.info(f"Connected to Elasticsearch at {ES_HOST}")
        return es
    except Exception as e:
        logger.error(f"Error connecting to Elasticsearch: {e}")
        sys.exit(1)

async def get_all_duplicate_ids(es):
    """
    Get ALL id_ara values with duplicates (doc_count >= 2) without batching
    """
    try:
        logger.info("Searching for documents with duplicate id_ara values (getting all)...")
        resp = await es.search(
            index=INDEX,
            size=0,
            body={
                "aggs": {
                    "dupes": {
                        "terms": {
                            "field": "id_ara",
                            "min_doc_count": 2,
                            "size": 10000000  # Very large number to get all duplicates
                        }
                    }
                }
            }
        )
        duplicate_ids = [(b["key"], b["doc_count"]) for b in resp["aggregations"]["dupes"]["buckets"]]
        logger.info(f"Found {len(duplicate_ids)} id_ara values with duplicates.")
        return duplicate_ids
    except Exception as e:
        logger.error(f"Error retrieving duplicate IDs: {e}")
        return []

async def analyze_duplicates(es, duplicate_ids):
    """
    Analyze and log detailed information about duplicates
    """
    try:
        total_docs = 0
        total_duplicates = 0
        
        # Count total docs and total duplicates
        for id_ara, count in duplicate_ids:
            total_docs += count
            total_duplicates += (count - 1)  # subtract 1 since we keep one copy
        
        # Get overall statistics
        duplicate_counts = [count for _, count in duplicate_ids]
        max_dupes = max(duplicate_counts) if duplicate_counts else 0
        avg_dupes = sum(duplicate_counts) / len(duplicate_counts) if duplicate_counts else 0
        
        # Log the summary statistics
        logger.info(f"=== DUPLICATE ANALYSIS ===")
        logger.info(f"Total unique id_ara values with duplicates: {len(duplicate_ids)}")
        logger.info(f"Total documents involved in deduplication: {total_docs}")
        logger.info(f"Total duplicate documents to remove: {total_duplicates}")
        logger.info(f"Average duplicates per id_ara: {avg_dupes:.2f}")
        logger.info(f"Maximum duplicates for a single id_ara: {max_dupes}")
        
        # Log the top 10 most duplicated ids
        if duplicate_ids:
            most_duplicated = sorted(duplicate_ids, key=lambda x: x[1], reverse=True)[:10]
            logger.info(f"Top 10 most duplicated id_ara values:")
            for id_ara, count in most_duplicated:
                logger.info(f"  id_ara={id_ara}: {count} documents ({count-1} duplicates)")
                
                # Get one sample document to show its details
                sample = await es.search(
                    index=INDEX,
                    body={
                        "query": {
                            "term": {"id_ara": id_ara}
                        }
                    },
                    size=1
                )
                
                if sample["hits"]["hits"]:
                    doc = sample["hits"]["hits"][0]["_source"]
                    logger.info(f"  Sample document fields: {', '.join(doc.keys())}")
        
        logger.info(f"=========================")
        
        # Write detailed duplication info to a file
        duplicate_csv_path = "duplicate_analysis.csv"
        with open(duplicate_csv_path, 'w') as f:
            f.write("id_ara,doc_count,duplicates_to_remove\n")
            for id_ara, count in sorted(duplicate_ids, key=lambda x: x[1], reverse=True):
                f.write(f"{id_ara},{count},{count-1}\n")
        
        logger.info(f"Detailed duplicate analysis written to {duplicate_csv_path}")
        
        return total_duplicates
        
    except Exception as e:
        logger.error(f"Error analyzing duplicates: {e}")
        return 0

async def delete_duplicates(es, id_ara_value, idx=None, total=None):
    """
    For a given id_ara, fetch all docs and keep only one (the first returned)
    """
    try:
        # Fetch all matching docs
        search_resp = await es.search(
            index=INDEX,
            body={
                "query": {
                    "term": {"id_ara": id_ara_value}
                }
            },
            size=1000  # adjust if you expect more than 1k duplicates for a single id
        )

        hits = search_resp["hits"]["hits"]
        total_hits = len(hits)
        
        # nothing to do if fewer than 2
        if total_hits < 2:
            logger.warning(f"Expected duplicates for id_ara={id_ara_value} but found only {total_hits} document")
            return 0

        # Keep the first document, delete the rest
        keep_doc = hits[0]
        logger.info(f"Keeping document {keep_doc['_id']} for id_ara={id_ara_value}")
        
        # delete the rest
        to_delete = hits[1:]
        
        # Use gather to delete documents in parallel
        delete_tasks = []
        for doc in to_delete:
            doc_id = doc["_id"]
            if not DRY_RUN:
                delete_tasks.append(es.delete(index=INDEX, id=doc_id))
            else:
                logger.info(f"DRY RUN: Would delete document {doc_id} for id_ara={id_ara_value}")
        
        # Execute all deletes for this ID in parallel if not in dry run mode
        if delete_tasks:
            await asyncio.gather(*delete_tasks)
            deleted_count = len(delete_tasks)
        else:
            deleted_count = len(to_delete)
        
        # Progress tracking
        if idx is not None and total is not None:
            if idx % 10 == 0 or idx == 1 or idx == total:
                logger.info(f"Progress: {idx}/{total} ({(idx/total)*100:.1f}%)")

        if deleted_count > 0:
            logger.info(f"id_ara={id_ara_value}: deleted {deleted_count} duplicates")
            
        return deleted_count
    except Exception as e:
        logger.error(f"Error processing id_ara={id_ara_value}: {e}")
        return 0

async def validate_index(es):
    """Validate that the index exists and contains the expected fields"""
    try:
        if not await es.indices.exists(index=INDEX):
            logger.error(f"Index '{INDEX}' does not exist")
            return False
        
        # Count total documents
        count = await es.count(index=INDEX)
        total_docs = count['count']
        logger.info(f"Total documents in '{INDEX}' index: {total_docs}")
        
        # Check if there are any duplicates
        aggs_resp = await es.search(
            index=INDEX,
            size=0,
            body={
                "aggs": {
                    "unique_ids": {
                        "cardinality": {
                            "field": "id_ara"
                        }
                    }
                }
            }
        )
        unique_ids = aggs_resp["aggregations"]["unique_ids"]["value"]
        logger.info(f"Unique id_ara values: {unique_ids}")
        logger.info(f"Potential duplicates: {total_docs - unique_ids}")
        
        return True
    except Exception as e:
        logger.error(f"Error validating index: {e}")
        return False

async def process_batch(es, id_batch, start_idx, total_count):
    """Process a batch of id_ara values with tasks"""
    tasks = []
    for i, item in enumerate(id_batch):
        idx = start_idx + i
        id_ara = item[0]  # Extract id_ara from the tuple
        tasks.append(delete_duplicates(es, id_ara, idx, total_count))
    
    # Run all tasks and collect results
    results = await asyncio.gather(*tasks)
    return sum(results)

async def main_async():
    """Main async function to handle the deduplication process"""
    logger.info("Starting duplicate document cleanup for 'ara' index")
    
    if DRY_RUN:
        logger.info("DRY RUN MODE: No documents will be deleted")
    
    start_time = time.time()
    
    # Connect to Elasticsearch
    es = await connect_elasticsearch()
    
    try:
        # Validate index before proceeding
        if not await validate_index(es):
            logger.error("Index validation failed. Exiting.")
            sys.exit(1)
        
        # Get duplicate IDs - all of them, with counts
        dupes_with_counts = await get_all_duplicate_ids(es)
        
        if not dupes_with_counts:
            logger.info("No duplicates found. Nothing to do.")
            return
        
        # Analyze and log duplicate information
        expected_deletions = await analyze_duplicates(es, dupes_with_counts)
        
        # Extract just the id_ara values for processing
        dupes = [d[0] for d in dupes_with_counts]
        
        # Process duplicates in batches
        total_dupes = len(dupes)
        total_deleted = 0
        
        logger.info(f"Processing {total_dupes} id_ara values with duplicates (max {MAX_CONCURRENT} concurrently)...")
        
        # Process in batches of MAX_CONCURRENT
        for i in range(0, total_dupes, MAX_CONCURRENT):
            batch = [(dupes[j], dupes_with_counts[j][1]) for j in range(i, min(i+MAX_CONCURRENT, total_dupes))]
            logger.info(f"Processing batch {i+1}-{i+len(batch)} of {total_dupes}")
            
            # Process this batch concurrently
            deleted = await process_batch(es, batch, i+1, total_dupes)
            total_deleted += deleted
            
            logger.info(f"Batch complete: deleted {deleted} documents (running total: {total_deleted}/{expected_deletions})")
        
        # Summary
        elapsed_time = time.time() - start_time
        if DRY_RUN:
            logger.info(f"\nDRY RUN completed. Would have deleted {total_deleted} duplicate documents in {elapsed_time:.2f} seconds")
        else:
            logger.info(f"\nCleanup completed. Total documents deleted: {total_deleted} in {elapsed_time:.2f} seconds")
        
        # Final validation
        if not DRY_RUN:
            logger.info("Performing final validation...")
            await validate_index(es)
            
    finally:
        # Close the Elasticsearch client
        await es.close()

def main():
    """Entry point that runs the async main function"""
    asyncio.run(main_async())

if __name__ == "__main__":
    main() 