#!/usr/bin/env python3
import os
import asyncio
import logging
import argparse
import time
from typing import Dict, Any, List
from dotenv import load_dotenv

from elasticsearch import AsyncElasticsearch

load_dotenv()

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('es_id_sync.log')
    ]
)

logger = logging.getLogger("es_id_sync")

class ElasticsearchIDSynchronizer:
    """Service for synchronizing id_ghavanin to id_edarehoquqy field."""
    
    def __init__(
        self,
        hosts: List[str] = None,
        index_name: str = "edarehoquqy",
        username: str = None,
        password: str = None,
        timeout: int = 30,
        batch_size: int = 100,
        retry_on_timeout: bool = True,
        max_retries: int = 4,
    ):
        """
        Initialize the Elasticsearch service.
        
        Args:
            hosts: List of Elasticsearch hosts
            index_name: Name of the index to use
            username: Elasticsearch username
            password: Elasticsearch password
            timeout: Request timeout in seconds
            batch_size: Number of documents to process at once
            retry_on_timeout: Whether to retry on timeout
            max_retries: Maximum number of retries for failed requests
        """
        # Check for ES_URL first, then fall back to ELASTICSEARCH_HOST
        es_url = os.environ.get("ES_URL")
        if es_url and not hosts:
            self.hosts = [es_url]
        else:
            self.hosts = hosts or [os.environ.get("ELASTICSEARCH_HOST", "http://localhost:9200")]
            
        self.index_name = index_name
        
        # Check for ES_PASSWORD first, then fall back to ELASTICSEARCH_PASSWORD
        es_password = os.environ.get("ES_PASSWORD")
        if es_password and not password:
            self.password = es_password
        else:
            self.password = password or os.environ.get("ELASTICSEARCH_PASSWORD")
            
        self.username = username or os.environ.get("ELASTICSEARCH_USERNAME", "elastic")
        
        self.timeout = timeout
        self.batch_size = batch_size
        self.retry_on_timeout = retry_on_timeout
        self.max_retries = max_retries
        
        # Initialize the client
        self.client = AsyncElasticsearch(
            hosts=self.hosts,
            basic_auth=(self.username, self.password) if self.username and self.password else None,
            request_timeout=self.timeout,
            retry_on_timeout=self.retry_on_timeout,
            max_retries=self.max_retries
        )
    
    async def sync_id_ghavanin_to_edarehoquqy(self) -> Dict[str, int]:
        """
        Sync id_ghavanin values to id_edarehoquqy field.
        
        Returns:
            Dict with statistics about the operation
        """
        start_time = time.time()
        logger.info(f"Starting synchronization of id_ghavanin to id_edarehoquqy fields in index {self.index_name}")
        
        # Stats to track progress
        stats = {
            "total_documents": 0,
            "documents_with_id_ghavanin": 0,
            "documents_updated": 0,
            "documents_already_synced": 0,
            "errors": 0,
            "runtime_seconds": 0
        }
        
        # Initialize scroll to process all documents
        query = {
            "query": {
                "exists": {
                    "field": "id_ghavanin"
                }
            },
            "_source": ["id_ghavanin", "id_edarehoquqy"]
        }
        
        try:
            # Get total document count
            count_response = await self.client.count(index=self.index_name)
            stats["total_documents"] = count_response.get("count", 0)
            logger.info(f"Total documents in index: {stats['total_documents']}")
            
            # Start scroll
            scroll_response = await self.client.search(
                index=self.index_name,
                body=query,
                scroll="5m",
                size=self.batch_size
            )
            
            scroll_id = scroll_response.get("_scroll_id")
            hits = scroll_response["hits"]["hits"]
            
            # Process batches
            while hits:
                stats["documents_with_id_ghavanin"] += len(hits)
                logger.info(f"Processing batch of {len(hits)} documents")
                
                # Process each document in the batch
                for hit in hits:
                    doc_id = hit["_id"]
                    source = hit["_source"]
                    
                    # Check if document has id_ghavanin
                    if "id_ghavanin" in source:
                        id_ghavanin = source["id_ghavanin"]
                        
                        # Check if id_edarehoquqy already matches id_ghavanin
                        if "id_edarehoquqy" in source and source["id_edarehoquqy"] == id_ghavanin:
                            stats["documents_already_synced"] += 1
                            continue
                        
                        # Update document to copy id_ghavanin to id_edarehoquqy
                        try:
                            update_response = await self.client.update(
                                index=self.index_name,
                                id=doc_id,
                                body={
                                    "doc": {
                                        "id_edarehoquqy": id_ghavanin
                                    }
                                }
                            )
                            
                            if update_response.get("result") == "updated":
                                stats["documents_updated"] += 1
                                if stats["documents_updated"] % 100 == 0:
                                    logger.info(f"Updated {stats['documents_updated']} documents so far")
                        except Exception as e:
                            stats["errors"] += 1
                            logger.error(f"Error updating document {doc_id}: {str(e)}")
                
                # Get next batch
                try:
                    scroll_response = await self.client.scroll(
                        scroll_id=scroll_id,
                        scroll="5m"
                    )
                    
                    scroll_id = scroll_response.get("_scroll_id")
                    hits = scroll_response["hits"]["hits"]
                except Exception as e:
                    logger.error(f"Error in scroll: {str(e)}")
                    break
            
            # Clean up scroll
            if scroll_id:
                try:
                    await self.client.clear_scroll(scroll_id=scroll_id)
                except Exception as e:
                    logger.warning(f"Error clearing scroll: {str(e)}")
        
        except Exception as e:
            logger.error(f"Synchronization failed: {str(e)}")
            stats["errors"] += 1
        
        # Calculate runtime
        runtime = time.time() - start_time
        stats["runtime_seconds"] = runtime
        
        # Log summary
        logger.info(f"Synchronization completed in {runtime:.2f} seconds:")
        logger.info(f"  Total documents in index: {stats['total_documents']}")
        logger.info(f"  Documents with id_ghavanin: {stats['documents_with_id_ghavanin']}")
        logger.info(f"  Documents already synced: {stats['documents_already_synced']}")
        logger.info(f"  Documents updated: {stats['documents_updated']}")
        logger.info(f"  Errors encountered: {stats['errors']}")
        
        return stats
    
    async def close(self):
        """Close the Elasticsearch client connection."""
        await self.client.close()


async def main():
    """Main entry point for the script."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description='Copy id_ghavanin values to id_edarehoquqy field in Elasticsearch documents.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    hosts = os.environ.get("ELASTICSEARCH_HOST")
    username = os.environ.get("ELASTICSEARCH_USERNAME")
    password = os.environ.get("ELASTICSEARCH_PASSWORD")
    index_name = "edarehoquqy"
    batch_size = 100
    
    # Initialize the synchronizer
    synchronizer = ElasticsearchIDSynchronizer(
        hosts=hosts,
        index_name=index_name,
        username=username,
        password=password,
        batch_size=batch_size
    )
    
    try:
        # Run the synchronization
        stats = await synchronizer.sync_id_ghavanin_to_edarehoquqy()
        
        # Determine exit code based on errors
        if stats.get("errors", 0) == 0:
            logger.info("Sync process completed successfully")
            exit_code = 0
        else:
            logger.error(f"Sync process completed with {stats['errors']} errors")
            exit_code = 1
        
    except Exception as e:
        logger.error(f"Sync process failed with exception: {str(e)}")
        exit_code = 2
    
    finally:
        # Close the client
        await synchronizer.close()
        
    return exit_code


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    exit(exit_code) 