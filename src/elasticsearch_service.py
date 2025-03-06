import os
import json
import asyncio
import httpx
from typing import Dict, Any, List, Optional
import logging
from datetime import datetime
from pydantic import BaseModel

from src.parser import ParsedContent

logger = logging.getLogger(__name__)


class ElasticsearchService:
    """Service for interacting with Elasticsearch."""
    
    def __init__(
        self,
        hosts: List[str] = None,
        index_name: str = "parsed_content",
        username: str = None,
        password: str = None,
        max_retries: int = 3,
        rate_limit: int = 20,  # Requests per minute
        timeout: int = 30,
        max_concurrent: int = 5,
    ):
        """
        Initialize the Elasticsearch service.
        
        Args:
            hosts: List of Elasticsearch hosts
            index_name: Name of the index to use
            username: Elasticsearch username
            password: Elasticsearch password
            max_retries: Maximum number of retries for failed requests
            rate_limit: Maximum requests per minute
            timeout: Request timeout in seconds
            max_concurrent: Maximum concurrent requests
        """
        self.hosts = hosts or [os.environ.get("ELASTICSEARCH_HOST", "http://localhost:9200")]
        self.index_name = index_name
        self.username = username or os.environ.get("ELASTICSEARCH_USERNAME")
        self.password = password or os.environ.get("ELASTICSEARCH_PASSWORD")
        
        self.max_retries = max_retries
        self.timeout = timeout
        
        # Rate limiting and concurrency control
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.rate_limit_period = 60 / rate_limit  # Time between requests in seconds
        self.last_request_time = 0
    
    async def ensure_index_exists(self) -> None:
        """Ensure the index exists with the correct mappings."""
        mapping = {
            "mappings": {
                "properties": {
                    "question": {"type": "text"},
                    "answer": {"type": "text"},
                    "date": {"type": "date"},
                    "metadata": {"type": "object"},
                    "embedding": {
                        "type": "dense_vector",
                        "dims": 3072,  # Dimensions for text-embedding-3-large
                        "index": True,
                        "similarity": "cosine"
                    }
                }
            }
        }
        
        # Check if index exists
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            auth = None
            if self.username and self.password:
                auth = (self.username, self.password)
            
            for host in self.hosts:
                try:
                    index_url = f"{host}/{self.index_name}"
                    response = await client.head(index_url, auth=auth)
                    
                    if response.status_code == 404:
                        # Create index with mapping
                        create_response = await client.put(
                            index_url,
                            json=mapping,
                            auth=auth
                        )
                        
                        if create_response.status_code not in (200, 201):
                            logger.error(f"Failed to create index: {create_response.status_code} {create_response.text}")
                            continue
                        
                        logger.info(f"Created index {self.index_name}")
                    
                    return  # Successfully checked/created index
                except Exception as e:
                    logger.error(f"Error checking/creating index on {host}: {str(e)}")
            
            raise Exception("Failed to ensure index exists on any host")
    
    async def index_document(self, document_id: str, content: ParsedContent, embedding: List[float]) -> bool:
        """
        Index a document in Elasticsearch.
        
        Args:
            document_id: ID of the document
            content: Parsed content to index
            embedding: Vector embedding of the content
            
        Returns:
            bool: True if successful, False otherwise
        """
        async with self.semaphore:
            # Rate limiting
            now = asyncio.get_event_loop().time()
            time_since_last_request = now - self.last_request_time
            if time_since_last_request < self.rate_limit_period:
                await asyncio.sleep(self.rate_limit_period - time_since_last_request)
            
            self.last_request_time = asyncio.get_event_loop().time()
            
            # Prepare document
            document = {
                "question": content.question,
                "answer": content.answer,
                "date": content.date.isoformat(),
                "metadata": content.metadata,
                "embedding": embedding
            }
            
            # Make the API request
            for attempt in range(self.max_retries):
                try:
                    async with httpx.AsyncClient(timeout=self.timeout) as client:
                        auth = None
                        if self.username and self.password:
                            auth = (self.username, self.password)
                        
                        for host in self.hosts:
                            try:
                                response = await client.put(
                                    f"{host}/{self.index_name}/_doc/{document_id}",
                                    json=document,
                                    auth=auth
                                )
                                
                                if response.status_code in (200, 201):
                                    logger.info(f"Successfully indexed document {document_id}")
                                    return True
                                else:
                                    logger.error(f"Error indexing document: {response.status_code} {response.text}")
                            except Exception as e:
                                logger.error(f"Error indexing document on {host}: {str(e)}")
                        
                        # If we get here, all hosts failed
                        if attempt < self.max_retries - 1:
                            wait_time = 2 ** attempt
                            logger.info(f"All hosts failed, retrying in {wait_time} seconds")
                            await asyncio.sleep(wait_time)
                        else:
                            return False
                except Exception as e:
                    logger.error(f"Attempt {attempt+1}/{self.max_retries} failed: {str(e)}")
                    if attempt == self.max_retries - 1:
                        return False
                    await asyncio.sleep(2 ** attempt)  # Exponential backoff
            
            return False 