import os
import logging
import asyncio
from typing import Dict, Any, List, Optional, Tuple
from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import async_bulk

from src.parser import ParsedContent

logger = logging.getLogger(__name__)


class ElasticsearchService:
    """Service for interacting with Elasticsearch using the official async client."""
    
    def __init__(
        self,
        hosts: List[str] = None,
        index_name: str = "edarehoquqy",
        username: str = None,
        password: str = None,
        timeout: int = 60,  # Increased from 30 to 60 seconds
        bulk_size: int = 50,  # Number of documents to bulk insert at once
        retry_on_timeout: bool = True,
        max_retries: int = 6,  # Increased from 4 to 6
    ):
        """
        Initialize the Elasticsearch service.
        
        Args:
            hosts: List of Elasticsearch hosts
            index_name: Name of the index to use
            username: Elasticsearch username
            password: Elasticsearch password
            timeout: Request timeout in seconds
            bulk_size: Number of documents to bulk insert at once
            retry_on_timeout: Whether to retry on timeout
            max_retries: Maximum number of retries for failed requests
        """
        # Check for ES_URL first, then fall back to ELASTICSEARCH_HOST
        self.hosts = hosts or [os.getenv("ELASTICSEARCH_HOST")]
            
        self.index_name = index_name
        
        # Check for ES_PASSWORD first, then fall back to ELASTICSEARCH_PASSWORD
        self.password = password or os.environ.get("ELASTICSEARCH_PASSWORD")
            
        self.username = username or os.environ.get("ELASTICSEARCH_USERNAME", "elastic")
        
        self.timeout = timeout
        self.bulk_size = bulk_size
        self.retry_on_timeout = retry_on_timeout
        self.max_retries = max_retries
        
        # Initialize the client
        self.client = AsyncElasticsearch(
            hosts=self.hosts,
            basic_auth=(self.username, self.password) if self.username and self.password else None,
            request_timeout=self.timeout,
            retry_on_timeout=self.retry_on_timeout,
            max_retries=self.max_retries,
            retry_on_status=[429, 500, 502, 503, 504]  # Add retry on specific HTTP status codes
        )
        
        # Bulk operation buffer
        self.bulk_buffer = []
    
    async def ensure_index_exists(self) -> None:
        """Ensure the index exists with the correct mappings."""
        mapping = {
            "mappings": {
                "properties": {
                    "question": {
                        "type": "text",
                        "term_vector": "with_positions_offsets",
                        "analyzer": "persian",
                        "similarity": "BM25"
                    },
                    "answer": {
                        "type": "text",
                        "term_vector": "with_positions_offsets",
                        "analyzer": "persian",
                        "similarity": "BM25"
                    },
                    "content": {
                        "type": "text",
                        "term_vector": "with_positions_offsets",
                        "analyzer": "persian",
                        "similarity": "BM25"
                    },
                    "embedding": {
                        "type": "dense_vector",
                        "dims": 3072,  # Dimensions for text-embedding-3-large
                        "index": True,
                        "similarity": "cosine",
                        "index_options": {
                            "type": "hnsw",
                            "m": 32,
                            "ef_construction": 200
                        }
                    },
                    "id_edarehoquqy": {
                        "type": "keyword"
                    },
                    "metadata": {
                        "properties": {
                            "file_id": {
                                "type": "keyword"
                            },
                            "file_number": {
                                "type": "keyword",
                                "null_value": "نامشخص"
                            },
                            "opinion_number": {
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
                            }
                        }
                    }
                }
            },
            "settings": {
                "index": {
                    "refresh_interval": "5s",  # Optimize for bulk indexing
                    "number_of_shards": 1,
                    "number_of_replicas": 1
                }
            }
        }
        
        # Check if index exists
        try:
            exists = await self.client.indices.exists(index=self.index_name)
            if not exists:
                # Create index with mapping
                await self.client.indices.create(
                    index=self.index_name,
                    body=mapping
                )
                logger.info(f"Created index {self.index_name}")
            else:
                logger.info(f"Index {self.index_name} already exists")
        except Exception as e:
            logger.error(f"Error checking/creating index: {str(e)}")
            raise
    
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
        # Prepare document
        document = {
            "question": content.question,
            "answer": content.answer,
            "content": content.content,
            "id_edarehoquqy": document_id,
            "metadata": content.metadata.model_dump(),
            "embedding": embedding
        }
        
        # Add to bulk buffer
        self.bulk_buffer.append({
            "_index": self.index_name,
            "_source": document
        })
        
        # If buffer is full, flush it
        if len(self.bulk_buffer) >= self.bulk_size:
            return await self._flush_bulk_buffer()
        
        return True
    
    async def _flush_bulk_buffer(self) -> bool:
        """
        Flush the bulk buffer to Elasticsearch.
        
        Returns:
            bool: True if successful, False otherwise
        """
        if not self.bulk_buffer:
            return True
        
        # Make a copy of the buffer and clear it
        documents_to_index = self.bulk_buffer.copy()
        self.bulk_buffer = []
        
        max_attempts = 3
        current_attempt = 0
        
        while current_attempt < max_attempts:
            try:
                current_attempt += 1
                
                # Use the async_bulk helper
                success, errors = await async_bulk(
                    client=self.client,
                    actions=documents_to_index,
                    stats_only=False,
                    raise_on_error=False,
                    max_retries=self.max_retries,
                    initial_backoff=2,  # Start with a 2-second backoff
                    max_backoff=60      # Maximum backoff of 60 seconds
                )
                
                if errors:
                    logger.error(f"Bulk indexing had {len(errors)} errors")
                    # Log the first few errors
                    for i, error in enumerate(errors[:3]):
                        logger.error(f"Error {i+1}: {error}")
                    
                    # If this was the last attempt, return partial success
                    if current_attempt >= max_attempts:
                        logger.warning(f"Giving up after {max_attempts} attempts")
                        return len(errors) < len(documents_to_index)
                    
                    # Otherwise, retry with the failed documents
                    failed_ids = set()
                    for error in errors:
                        if 'index' in error and '_id' in error['index']:
                            failed_ids.add(error['index']['_id'])
                    
                    # Filter out successful documents and retry only the failed ones
                    documents_to_index = [doc for doc in documents_to_index 
                                         if doc.get('_id', '') in failed_ids]
                    
                    logger.info(f"Retrying {len(documents_to_index)} failed documents (attempt {current_attempt}/{max_attempts})")
                    await asyncio.sleep(2 * current_attempt)  # Exponential backoff
                else:
                    logger.info(f"Successfully bulk indexed {success} documents")
                    return True
                    
            except Exception as e:
                logger.error(f"Error in bulk indexing (attempt {current_attempt}/{max_attempts}): {str(e)}")
                
                # If this was the last attempt, return failure
                if current_attempt >= max_attempts:
                    logger.warning(f"Giving up after {max_attempts} attempts")
                    return False
                
                # Otherwise, wait and retry
                retry_delay = 5 * current_attempt  # Exponential backoff
                logger.info(f"Retrying bulk indexing in {retry_delay} seconds")
                await asyncio.sleep(retry_delay)
        
        return False
    
    async def flush_all(self) -> bool:
        """
        Flush all remaining documents in the buffer.
        
        Returns:
            bool: True if successful, False otherwise
        """
        return await self._flush_bulk_buffer()
    
    async def bulk_index_documents(self, documents: List[Tuple[str, ParsedContent, List[float]]]) -> bool:
        """
        Bulk index multiple documents.
        
        Args:
            documents: List of (document_id, content, embedding) tuples
            
        Returns:
            bool: True if all documents were indexed successfully, False otherwise
        """
        # Prepare all documents at once
        for doc_id, content, embedding in documents:
            document = {
                "question": content.question,
                "answer": content.answer,
                "content": content.content,
                "id_ghavanin": doc_id,
                "metadata": content.metadata.model_dump(),
                "embedding": embedding
            }
            
            self.bulk_buffer.append({
                "_index": self.index_name,
                "_id": doc_id,
                "_source": document
            })
        
        # Flush the buffer
        return await self.flush_all()
    
    async def search_by_vector(self, embedding: List[float], k: int = 5) -> List[Dict[str, Any]]:
        """
        Search for documents by vector similarity.
        
        Args:
            embedding: Vector embedding to search with
            k: Number of results to return
            
        Returns:
            List[Dict[str, Any]]: List of search results
        """
        query = {
            "knn": {
                "field": "embedding",
                "query_vector": embedding,
                "k": k,
                "num_candidates": k * 2
            },
            "_source": ["question", "answer", "content", "metadata"]
        }
        
        try:
            response = await self.client.search(
                index=self.index_name,
                body=query
            )
            
            return [hit["_source"] for hit in response["hits"]["hits"]]
        except Exception as e:
            logger.error(f"Error in vector search: {str(e)}")
            return []
    
    async def search_by_text(self, query_text: str, max_results: int = None) -> List[Dict[str, Any]]:
        """
        Search for documents by text using pagination for reliable retrieval of matches.
        
        Args:
            query_text: Text to search for
            max_results: Maximum number of results to return. If None, returns all matching documents.
            
        Returns:
            List[Dict[str, Any]]: List of search results with no duplicates
        """
        query = {
            "query": {
                "bool": {
                    "should": [
                        # Exact phrase matches (highest priority)
                        {
                            "multi_match": {
                                "query": query_text,
                                "fields": ["question^4", "answer^3", "content^2"],
                                "type": "phrase",
                                "analyzer": "persian",
                                "boost": 3
                            }
                        },
                        # Term proximity search
                        {
                            "multi_match": {
                                "query": query_text,
                                "fields": ["question^3", "answer^2", "content"],
                                "type": "phrase_prefix",
                                "analyzer": "persian",
                                "boost": 2
                            }
                        },
                        # Fuzzy matching for typos and variations
                        {
                            "multi_match": {
                                "query": query_text,
                                "fields": ["question^2", "answer^1.5", "content"],
                                "type": "best_fields",
                                "analyzer": "persian",
                                "fuzziness": "AUTO",
                                "boost": 1
                            }
                        }
                    ],
                    "minimum_should_match": 1,
                    # Filter to ensure some minimum relevance
                    "filter": [
                        {
                            "multi_match": {
                                "query": query_text,
                                "fields": ["question", "answer", "content"],
                                "operator": "and",
                                "minimum_should_match": "70%"
                            }
                        }
                    ]
                }
            },
            "_source": ["question", "answer", "content", "metadata", "id_ghavanin", "id_edarehoquqy"],
            # Track total hits for accurate count
            "track_total_hits": True,
            # Use a larger page size for efficiency
            "size": 1000,
            # Sort for consistent results
            "sort": ["_score", {"id_edarehoquqy": "asc"}]
        }
        
        try:
            # First, get the total number of matching documents
            initial_response = await self.client.search(
                index=self.index_name,
                body={
                    "query": query["query"],
                    "track_total_hits": True,
                    "size": 0  # Just get the count, no actual documents
                }
            )
            
            total_hits = initial_response["hits"]["total"]["value"]
            logger.info(f"Total matching documents: {total_hits}")
            
            # If no results, return empty list
            if total_hits == 0:
                return []
                
            all_results = []
            seen_ids = set()  # Track seen document IDs for deduplication
            from_position = 0
            page_size = 1000
            
            # Use pagination instead of scroll
            while len(all_results) < total_hits:
                if max_results is not None and len(all_results) >= max_results:
                    break
                    
                # Update from position in the query
                search_query = query.copy()
                search_query["from"] = from_position
                search_query["size"] = page_size
                
                response = await self.client.search(
                    index=self.index_name,
                    body=search_query
                )
                
                # Break if no more hits
                hits = response["hits"]["hits"]
                if not hits:
                    break
                
                # Process batch with deduplication
                for hit in hits:
                    doc_id = None
                    source = hit["_source"]
                    
                    # Try to get a unique ID
                    if "id_edarehoquqy" in source:
                        doc_id = source["id_edarehoquqy"]
                    elif "id_ghavanin" in source:
                        doc_id = source["id_ghavanin"]
                    else:
                        doc_id = hit["_id"]
                        
                    # Skip if we've seen this ID before
                    if doc_id in seen_ids:
                        continue
                        
                    seen_ids.add(doc_id)
                    source["_score"] = hit["_score"]
                    all_results.append(source)
                    
                    # Stop if we've reached max_results
                    if max_results is not None and len(all_results) >= max_results:
                        break
                
                # Move to next page
                from_position += page_size
                
                # Log progress for large result sets
                if len(all_results) % 5000 == 0:
                    logger.info(f"Retrieved {len(all_results)} results so far")
            
            logger.info(f"Retrieved {len(all_results)} total deduplicated results")
            return all_results
            
        except Exception as e:
            logger.error(f"Error in text search: {str(e)}")
            return []
    
    async def close(self):
        """Close the Elasticsearch client connection."""
        await self.client.close() 

    
    async def __aenter__(self):
        await self.ensure_index_exists()
        return self
    
    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.close()
        self.client = None
