import traceback
import os
import logging
import asyncio
from typing import AsyncGenerator, Dict, Any, List, Optional, Tuple
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
    
    def get_keyword_query(
        self,
        raw_query: str,
        use_semantic: bool = False,
        index_name: str = None,
    ) -> Dict[str, Any]:
        """
        Build a simple but effective query to find relevant documents.
        
        Args:
            raw_query: The search query (string or list of strings)
            use_semantic: Whether to use semantic search
            index_name: The index name to determine source fields
            
        Returns:
            Dict with the query
        """
        # For string inputs, treat entire string as a single phrase
        phrases = [raw_query]
        if isinstance(raw_query, list):
            phrases = raw_query
            
        # Filter out empty phrases
        phrases = [p.strip() for p in phrases if p and p.strip()]
        phrase_count = len(phrases)
        logger.info(f"Processing search with {phrase_count} phrases")
        
        if phrase_count == 0:
            logger.warning("No valid phrases in query")
            return {"query": {"match_none": {}}}

        # Create a query that uses simple match_phrase clauses
        query = {
            "query": {
                "bool": {
                    "should": [],
                    "minimum_should_match": 1
                }
            }
        }
        
        # Determine source fields based on index name
        index_name = index_name or self.index_name
        if index_name == "ara":
            query["_source"] = ["id_ara", "title", "content", "message", "documents", "metadata", "date"]
        else:  # Default for edarehoquqy and other indices
            query["_source"] = ["question", "answer", "content", "metadata", "id_ghavanin", "id_edarehoquqy"]
        
        # Add phrase matches for each field
        for phrase in phrases:
            if index_name == "ara":
                # Add match_phrase queries for ARA index with field-specific boosts
                query["query"]["bool"]["should"].extend([
                    {"match_phrase": {"title": {"query": phrase, "boost": 5}}},
                    {"match_phrase": {"content": {"query": phrase, "boost": 3}}},
                    {"match_phrase": {"message": {"query": phrase, "boost": 2}}},
                    {"match_phrase": {"documents": {"query": phrase, "boost": 1}}}
                ])
            else:
                # Add match_phrase queries for other indices with field-specific boosts
                query["query"]["bool"]["should"].extend([
                    {"match_phrase": {"question": {"query": phrase, "boost": 5}}},
                    {"match_phrase": {"answer": {"query": phrase, "boost": 3}}},
                    {"match_phrase": {"content": {"query": phrase, "boost": 2}}}
                ])
        
        return query

    async def search_by_text_batch(
        self,
        raw_query: str,
        max_results: Optional[int] = None,
        use_semantic: bool = False,
        min_score: float = 1.0,
        index_name: str = None
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """
        Search for documents matching the query and return results in batches.
        
        Args:
            raw_query: The search query
            max_results: Maximum number of results to return
            use_semantic: Whether to use semantic search
            min_score: Minimum score threshold
            index_name: Index name to search (defaults to self.index_name)
            
        Yields:
            Dict with search results
        """
        scroll_id = None
        
        try:
            # Determine which index to use
            index_to_search = index_name or self.index_name
            
            # Build the search query
            search_query = self.get_keyword_query(raw_query, use_semantic=use_semantic, index_name=index_to_search)
            search_query["size"] = 100
            search_query["track_total_hits"] = True
            search_query["sort"] = [{"_score": {"order": "desc"}}]
            
            # Add min_score filter
            if min_score > 0:
                search_query["min_score"] = min_score
            
            # Execute search
            resp = await self.client.search(
                index=index_to_search,
                body=search_query,
                scroll="1m",
                timeout="120s"
            )
            
            # Get scroll ID and total hits
            scroll_id = resp.get("_scroll_id")
            total_hits = resp["hits"]["total"]["value"]
            logger.info(f"Found {total_hits} total hits for query: {raw_query}")
            
            # Process results
            processed = 0
            relevant_count = 0
            seen_ids = set()
            
            # Continue scrolling until done
            while True:
                # Get hits from current response
                hits = resp["hits"]["hits"]
                if not hits:
                    break
                
                # Process hits
                batch = []
                for hit in hits:
                    # Check if we've reached max results
                    if max_results and relevant_count >= max_results:
                        break
                    
                    # Extract document data
                    source = hit["_source"]
                    score = hit["_score"]
                    
                    # Add _id to source for identification
                    if "_id" in hit and "_id" not in source:
                        source["_id"] = hit["_id"]
                    
                    # Get document ID based on index
                    if index_to_search == "ara":
                        doc_id = source.get("id_ara")
                        # If id_ara is missing, log a warning but continue
                        if not doc_id:
                            logger.warning(f"Document missing id_ara field: {source.keys()}")
                            continue
                    else:
                        doc_id = source.get("id_edarehoquqy") or source.get("id_ghavanin")
                        if not doc_id:
                            logger.warning(f"Document missing id_edarehoquqy or id_ghavanin field: {source.keys()}")
                            continue
                    
                    # Skip duplicate documents
                    if doc_id in seen_ids:
                        continue
                    
                    # Add to results
                    seen_ids.add(doc_id)
                    source["_score"] = score
                    batch.append(source)
                    processed += 1
                    relevant_count += 1
                
                # Yield batch if not empty
                if batch:
                    yield {
                        "total_count": total_hits,
                        "relevant_count": relevant_count,
                        "results": batch,
                        "precision": relevant_count / processed if processed > 0 else 0
                    }
                
                # Break if we've reached max results or no more hits
                if not hits or (max_results and relevant_count >= max_results):
                    break
                
                # Get next batch
                try:
                    resp = await self.client.scroll(scroll_id=scroll_id, scroll="1m")
                    scroll_id = resp.get("_scroll_id")
                except Exception as e:
                    logger.error(f"Error during scroll: {e}")
                    break
            
            # Log completion
            logger.info(f"Retrieved {relevant_count} documents out of {total_hits} total hits")
            
        except Exception as e:
            logger.error(f"Search error: {e}")
            yield {"results": [], "error": str(e)}
        
        finally:
            # Clear scroll context
            if scroll_id:
                try:
                    await self.client.clear_scroll(scroll_id=scroll_id)
                except Exception as e:
                    logger.warning(f"Failed to clear scroll: {e}")
    
    async def get_missing_documents(self, existing_ids: List[str], index_name: str = None) -> AsyncGenerator[Dict[str, Any], None]:
        """
        Get documents that are not present in Elasticsearch.
        
        Args:
            existing_ids: List of document IDs that are already indexed
            index_name: Index name to search (defaults to self.index_name)
            
        Returns:
            AsyncGenerator[Dict[str, Any], None]: Generator yielding documents missing from Elasticsearch
        """
        if not existing_ids:
            logger.warning("No existing IDs provided, cannot determine missing documents")
            return
            
        try:
            # Determine which index to use
            index_to_search = index_name or self.index_name
            
            # Convert to set for efficient lookup
            existing_ids_set = set(existing_ids)
            
            # Get total document count
            total_docs = await self.client.count(index=index_to_search)
            count_value = total_docs.get("count", 0)
            
            logger.info(f"Total documents in {index_to_search}: {count_value}")
            logger.info(f"Existing IDs: Length: {len(existing_ids_set)}")
            logger.info(f"Missing IDs: Length: {count_value - len(existing_ids_set)}")
            
            # If there are no missing documents, return early
            if count_value <= len(existing_ids_set):
                logger.info("No missing documents found")
                return
            
            # Determine ID field name based on index
            id_field = "id_ara" if index_to_search == "ara" else "id_edarehoquqy"
            
            # Determine source fields based on index
            source_fields = ["id_ara", "content", "message", "documents", "metadata", "date", "title"] if index_to_search == "ara" else ["id_edarehoquqy", "question", "answer", "content", "metadata"]
            
            # Query for documents that are NOT in the existing_ids_set
            query = {
                "query": {
                    "bool": {
                        "must_not": {
                            "terms": {
                                id_field: list(existing_ids_set)
                            }
                        }
                    }
                }, 
                "_source": source_fields,
                "size": 4000  # Fetch in larger batches to process more documents
            }

            # Get documents from elasticsearch using scroll API
            resp = await self.client.search(
                index=index_to_search,
                body=query,
                scroll="10m"  # Increase scroll time to handle larger batches
            )
            
            scroll_id = resp.get("_scroll_id")
            total_hits = resp["hits"]["total"]["value"]
            logger.info(f"Found {total_hits} documents not in the existing IDs list")
            processed_count = 0
            
            try:
                # Process initial batch
                for hit in resp["hits"]["hits"]:
                    source = hit["_source"]
                    # Ensure ID field exists
                    if id_field not in source:
                        logger.warning(f"Document missing {id_field} field, skipping: {source.keys()}")
                        continue
                    
                    processed_count += 1
                    yield source
                    
                # Continue scrolling until all results are retrieved
                while scroll_id and resp["hits"]["hits"]:
                    resp = await self.client.scroll(
                        scroll_id=scroll_id,
                        scroll="10m"  # Increase scroll time
                    )
                    scroll_id = resp.get("_scroll_id")
                    
                    for hit in resp["hits"]["hits"]:
                        source = hit["_source"]
                        
                        # Ensure ID field exists
                        if id_field not in source:
                            logger.warning(f"Document missing {id_field} field, skipping: {source.keys()}")
                            continue
                        
                        processed_count += 1
                        if processed_count % 1000 == 0:
                            logger.info(f"Processed {processed_count}/{total_hits} documents")
                        yield source
                
                logger.info(f"Finished processing all {processed_count} documents")
            except Exception as e:
                logger.error(f"Error processing search results: {e}")
            finally:
                # Clear scroll context if it exists
                if scroll_id:
                    try:
                        await self.client.clear_scroll(scroll_id=scroll_id)
                    except Exception as e:
                        logger.warning(f"Failed to clear scroll: {e}")
                        
        except Exception as e:
            logger.error(f"Error retrieving missing documents: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
    
    async def get_document(self, doc_id: str) -> Optional[Dict[str, Any]]:
        """
        Get a document by ID from Elasticsearch.
        
        Args:
            doc_id: ID of the document to retrieve
        
        Returns:
            Dict[str, Any]: Document data
        """
        try:
            resp = await self.client.get(index=self.index_name, id=doc_id, source=["question", "answer", "content", "metadata", "id_edarehoquqy"])
            return resp["_source"]
        except Exception as e:
            logger.error(f"Error getting document: {e}")
            return None
    

    async def close(self):
        """Close the Elasticsearch client connection."""
        await self.client.close() 

    
    async def __aenter__(self):
        await self.ensure_index_exists()
        return self
    
    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.close()
        self.client = None

