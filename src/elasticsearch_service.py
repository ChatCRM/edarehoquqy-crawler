import traceback
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
        
    def get_keyword_query(self, query_text: str) -> dict:
        # Precision-focused query with relevance prioritization
        return {
            "query": {
                "function_score": {
                    "query": {
                        "bool": {
                            "should": [
                                # Exact phrase matches (highest priority)
                                {
                                    "multi_match": {
                                        "query": query_text,
                                        "fields": ["question^5", "answer^4", "content^2"],
                                        "type": "phrase",
                                        "analyzer": "persian",
                                        "boost": 5
                                    }
                                },
                                # Close phrase matches with limited slop
                                {
                                    "multi_match": {
                                        "query": query_text,
                                        "fields": ["question^4", "answer^3", "content^1.5"],
                                        "type": "phrase",
                                        "slop": 3,  # Reduced slop for higher precision
                                        "analyzer": "persian",
                                        "boost": 3
                                    }
                                },
                                # Controlled term matching (balanced approach)
                                {
                                    "multi_match": {
                                        "query": query_text,
                                        "fields": ["question^3", "answer^2.5", "content^1"],
                                        "type": "best_fields",
                                        "operator": "and",  # Changed to AND for higher precision
                                        "minimum_should_match": "70%",  # Require most terms to match
                                        "analyzer": "persian",
                                        "boost": 2.5
                                    }
                                },
                                # Term proximity with limited prefix matching
                                {
                                    "multi_match": {
                                        "query": query_text,
                                        "fields": ["question^3", "answer^2.5", "content^1"],
                                        "type": "phrase_prefix",
                                        "analyzer": "persian",
                                        "max_expansions": 20,  # Limited expansions for precision
                                        "boost": 2
                                    }
                                },
                                # More controlled fuzzy matching
                                {
                                    "multi_match": {
                                        "query": query_text,
                                        "fields": ["question^2", "answer^1.5", "content"],
                                        "type": "best_fields",
                                        "analyzer": "persian",
                                        "fuzziness": 1,  # Stricter fuzziness
                                        "prefix_length": 3,  # First 3 chars must match exactly
                                        "boost": 1
                                    }
                                },
                                # Cross-field matching with higher threshold
                                {
                                    "multi_match": {
                                        "query": query_text,
                                        "fields": ["question", "answer", "content"],
                                        "type": "cross_fields",
                                        "analyzer": "persian",
                                        "operator": "and",  # Changed to AND
                                        "minimum_should_match": "70%",  # Higher threshold
                                        "boost": 1.5
                                    }
                                }
                            ],
                            "minimum_should_match": 1,
                            # Precision filter that ensures good relevance
                            "filter": [
                                {
                                    "multi_match": {
                                        "query": query_text,
                                        "fields": ["question", "answer", "content"],
                                        "operator": "and",  # Changed to AND for higher precision
                                        "minimum_should_match": "65%"  # Balanced threshold for relevance
                                    }
                                }
                            ]
                        }
                    },
                    # Boost by original score for natural ordering
                    "functions": [
                        {
                            "field_value_factor": {
                                "field": "_score",
                                "factor": 1.2,
                                "modifier": "ln2p",
                                "missing": 1
                            }
                        }
                    ],
                    "score_mode": "sum",
                    "boost_mode": "multiply"
                }
            },
            "_source": ["question", "answer", "content", "metadata", "id_ghavanin", "id_edarehoquqy"],
            "track_total_hits": True,
            "size": 1000,
            "sort": [
                "_score", 
                {"id_edarehoquqy": "asc"}
            ],
            "highlight": {
                "fields": {
                    "question": {"number_of_fragments": 2, "fragment_size": 150},
                    "answer": {"number_of_fragments": 2, "fragment_size": 150},
                    "content": {"number_of_fragments": 2, "fragment_size": 150}
                },
                "order": "score"
            }
        }
        
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
        
        # Check if index exists with retry logic
        retry_delay = 2
        max_attempts = 3
        for attempt in range(max_attempts):
            try:
                # Log connection attempt with host information
                hosts_str = ', '.join(self.hosts) if isinstance(self.hosts, list) else str(self.hosts)
                logger.info(f"Attempting to connect to Elasticsearch at {hosts_str} (attempt {attempt+1}/{max_attempts})")
                
                # First check if we can connect to Elasticsearch at all
                info = await self.client.info(request_timeout=10)
                logger.info(f"Successfully connected to Elasticsearch version {info.get('version', {}).get('number', 'unknown')}")
                
                # Now check if the index exists
                exists = await self.client.indices.exists(index=self.index_name, request_timeout=10)
                if not exists:
                    # Create index with mapping
                    logger.info(f"Creating index {self.index_name}...")
                    await self.client.indices.create(
                        index=self.index_name,
                        body=mapping,
                        request_timeout=30
                    )
                    logger.info(f"Created index {self.index_name}")
                    return
                else:
                    logger.info(f"Index {self.index_name} already exists")
                    return
                    
            except Exception as e:
                if attempt < max_attempts - 1:
                    # Log detailed exception info
                    logger.warning(f"Attempt {attempt+1}/{max_attempts} to connect to Elasticsearch failed: {str(e)}")
                    logger.warning(f"Exception type: {type(e).__name__}")
                    logger.warning(f"Exception details: {repr(e)}")
                    
                    wait_time = retry_delay * (2 ** attempt)  # Exponential backoff
                    logger.warning(f"Retrying in {wait_time} seconds...")
                    await asyncio.sleep(wait_time)
                else:
                    logger.error(f"Error checking/creating index after {max_attempts} attempts: {str(e)}")
                    logger.error(f"Exception type: {type(e).__name__}")
                    logger.error(f"Exception details: {repr(e)}")
                    # Don't raise the exception, just continue with partial functionality
                    return
    
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
    
    async def search_by_text_batch(self, query_text: str, max_results: int = None) -> List[Dict[str, Any]]:
        """
        Search for documents by text using the Elasticsearch scroll API to reliably retrieve matches in batches.
        
        Args:
            query_text: Text to search for
            max_results: Maximum number of results to return. If None, returns all matching documents.
            
        Returns:
            async generator yielding batches of search results with no duplicates
        """
        try:
            # Use our search query with a fixed batch size
            search_query = self.get_keyword_query(query_text)
            search_query["size"] = 100  # Fixed batch size
            search_query["track_total_hits"] = True  # Ensure we get the total hit count
            
            logger.info(f"Starting scroll search with query: {query_text}")
            
            # Initialize scroll search
            scroll_response = await self.client.search(
                index=self.index_name,
                body=search_query,
                scroll="2m"  # Keep scroll context alive for 2 minutes
            )
            
            scroll_id = scroll_response["_scroll_id"]
            total_docs = scroll_response["hits"]["total"]["value"]
            logger.info(f"Total matching documents available: {total_docs}")
            
            # Process results in batches
            processed_count = 0
            seen_ids = set()  # Track seen document IDs for deduplication
            
            while True:
                hits = scroll_response["hits"]["hits"]
                if not hits:
                    logger.info(f"No more results to process, reached end of scroll")
                    break
                
                # Process current batch
                batch_results = []
                
                for hit in hits:
                    # Break if we've reached max_results
                    if max_results is not None and processed_count >= max_results:
                        logger.info(f"Reached max_results limit of {max_results}")
                        break
                    
                    source = hit["_source"]
                    doc_id = source.get("id_edarehoquqy") or source.get("id_ghavanin") or hit["_id"]
                    
                    # Skip if we've seen this ID before
                    if doc_id in seen_ids:
                        continue
                    
                    seen_ids.add(doc_id)
                    source["_score"] = hit.get("_score")
                    batch_results.append(source)
                    processed_count += 1
                
                # Log batch progress
                logger.info(f"Processed batch with {len(batch_results)} documents, total so far: {processed_count}/{total_docs}")
                
                # Yield the current batch if there are results
                if batch_results:
                    # Include total count with first batch
                    if processed_count == len(batch_results):
                        # Add total as metadata with the first batch
                        yield {"total_count": total_docs, "results": batch_results}
                    else:
                        yield {"results": batch_results}
                
                # Break if we've processed all results or reached max_results
                if not hits or (max_results is not None and processed_count >= max_results):
                    break
                
                # Get the next batch
                try:
                    scroll_response = await self.client.scroll(
                        scroll_id=scroll_id,
                        scroll="2m"
                    )
                    scroll_id = scroll_response["_scroll_id"]
                except Exception as e:
                    logger.error(f"Error in scroll request: {str(e)}")
                    break
            
            logger.info(f"Search completed. Retrieved {processed_count} documents out of {total_docs} total matches")
            
            # Clean up scroll context
            try:
                if scroll_id:
                    await self.client.clear_scroll(scroll_id=scroll_id)
            except Exception as e:
                logger.warning(f"Error clearing scroll context: {str(e)}")
            
        except Exception as e:
            logger.error(f"Error in scroll search: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            return
    
    async def close(self):
        """Close the Elasticsearch client connection."""
        await self.client.close() 

    
    async def __aenter__(self):
        await self.ensure_index_exists()
        return self
    
    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.close()
        self.client = None

