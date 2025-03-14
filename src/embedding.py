import os
import asyncio
import logging
from typing import List, Dict, Any, Optional, Tuple, Set
import time
from openai import AsyncOpenAI
import random
import tiktoken

logger = logging.getLogger(__name__)


class EmbeddingService:
    """Service for generating embeddings using OpenAI's API."""
    
    def __init__(
        self,
        api_key: str = None,
        model: str = "text-embedding-3-large",
        max_retries: int = 4,
        rate_limit: int = 20,  # Requests per minute
        timeout: int = 30,
        batch_size: int = 1,  # Number of texts to batch in a single API call
        max_tokens: int = 8192,  # Maximum token limit for the model
    ):
        """
        Initialize the embedding service.
        
        Args:
            api_key: OpenAI API key (defaults to OPENAI_API_KEY env var)
            model: The embedding model to use
            max_retries: Maximum number of retries for failed requests
            rate_limit: Maximum requests per minute
            timeout: Request timeout in seconds
            batch_size: Number of texts to batch in a single API call
            max_tokens: Maximum token limit for the model
        """
        self.api_key = api_key or os.environ.get("OPENAI_API_KEY")
        if not self.api_key:
            raise ValueError("OpenAI API key is required")
        
        self.model = model
        self.max_retries = max_retries
        self.timeout = timeout
        self.batch_size = batch_size
        self.max_tokens = max_tokens
        
        # Initialize OpenAI client
        self.client = AsyncOpenAI(
            api_key=self.api_key,
            base_url="https://api.avalai.ir/v1",
            timeout=timeout
        )
        
        # Rate limiting
        self.semaphore = asyncio.Semaphore(rate_limit // 6 + 1)  # Allow bursts but average to rate limit
        self.rate_limit_period = 60 / rate_limit  # Time between requests in seconds
        self.last_request_time = 0
        
        # Set to store IDs of documents that exceed token limit
        self.oversized_document_ids = set()
        
        # Initialize tokenizer
        try:
            self.tokenizer = tiktoken.encoding_for_model("gpt-3.5-turbo")
        except:
            # Fallback to cl100k_base if model-specific encoding is not available
            self.tokenizer = tiktoken.get_encoding("cl100k_base")
    
    def estimate_token_count(self, text: str) -> int:
        """
        Estimate the number of tokens in a text.
        
        Args:
            text: The text to estimate tokens for
            
        Returns:
            int: Estimated token count
        """
        if not text:
            return 0
        
        # Use the tokenizer to count tokens
        tokens = self.tokenizer.encode(text)
        return len(tokens)
    
    async def get_embedding(self, text: str) -> List[float]:
        """
        Get embedding for a single text.
        
        Args:
            text: The text to embed
            
        Returns:
            List[float]: The embedding vector
        """
        # Check if text exceeds token limit
        token_count = self.estimate_token_count(text)
        if token_count > self.max_tokens:
            logger.warning(f"Text exceeds token limit ({token_count} > {self.max_tokens})")
            raise ValueError(f"Text exceeds token limit ({token_count} > {self.max_tokens})")
        
        async with self.semaphore:
            # Rate limiting
            now = asyncio.get_event_loop().time()
            time_since_last_request = now - self.last_request_time
            if time_since_last_request < self.rate_limit_period:
                await asyncio.sleep(self.rate_limit_period - time_since_last_request)
            
            self.last_request_time = asyncio.get_event_loop().time()
            
            # Make the API request with retries
            for attempt in range(self.max_retries):
                try:
                    response = await self.client.embeddings.create(
                        model=self.model,
                        input=text
                    )
                    
                    return response.data[0].embedding
                except Exception as e:
                    logger.error(f"Attempt {attempt+1}/{self.max_retries} failed: {str(e)}")
                    if attempt == self.max_retries - 1:
                        raise
                    
                    # Exponential backoff with jitter
                    backoff_time = min(2 ** attempt + (0.1 * random.random()), 60)
                    logger.info(f"Retrying in {backoff_time:.2f} seconds")
                    await asyncio.sleep(backoff_time)
            
            raise Exception("Failed to get embedding after maximum retries")
    
    async def get_embeddings_batch(self, texts: List[str]) -> List[List[float]]:
        """
        Get embeddings for multiple texts in a single API call.
        
        Args:
            texts: List of texts to embed
            
        Returns:
            List[List[float]]: List of embedding vectors
        """
        if not texts:
            return []
        
        # Check if any text exceeds token limit
        for i, text in enumerate(texts):
            token_count = self.estimate_token_count(text)
            if token_count > self.max_tokens:
                logger.warning(f"Text at index {i} exceeds token limit ({token_count} > {self.max_tokens})")
                raise ValueError(f"Text at index {i} exceeds token limit ({token_count} > {self.max_tokens})")
        
        async with self.semaphore:
            # Rate limiting
            now = asyncio.get_event_loop().time()
            time_since_last_request = now - self.last_request_time
            if time_since_last_request < self.rate_limit_period:
                await asyncio.sleep(self.rate_limit_period - time_since_last_request)
            
            self.last_request_time = asyncio.get_event_loop().time()
            
            # Make the API request with retries
            for attempt in range(self.max_retries):
                try:
                    response = await self.client.embeddings.create(
                        model=self.model,
                        input=texts
                    )
                    
                    # Sort embeddings by index to maintain original order
                    sorted_embeddings = sorted(response.data, key=lambda x: x.index)
                    return [item.embedding for item in sorted_embeddings]
                except Exception as e:
                    logger.error(f"Batch embedding attempt {attempt+1}/{self.max_retries} failed: {str(e)}")
                    if attempt == self.max_retries - 1:
                        raise
                    
                    # Exponential backoff with jitter
                    backoff_time = min(2 ** attempt + (0.1 * random.random()), 60)
                    logger.info(f"Retrying batch in {backoff_time:.2f} seconds")
                    await asyncio.sleep(backoff_time)
            
            raise Exception("Failed to get batch embeddings after maximum retries")
    
    async def process_texts_in_batches(self, texts: List[str], document_ids: Optional[List[str]] = None) -> Tuple[List[List[float]], List[int]]:
        """
        Process a list of texts in batches to get embeddings.
        
        Args:
            texts: List of texts to embed
            document_ids: Optional list of document IDs corresponding to the texts
            
        Returns:
            Tuple[List[List[float]], List[int]]: Tuple of (embedding vectors, indices of skipped documents)
        """
        if not texts:
            return [], []
        
        results = []
        skipped_indices = []
        
        # Check if we have document IDs
        has_doc_ids = document_ids is not None and len(document_ids) == len(texts)
        
        # Process in batches
        for i in range(0, len(texts), self.batch_size):
            batch = texts[i:i + self.batch_size]
            batch_indices = list(range(i, min(i + self.batch_size, len(texts))))
            
            # Filter out texts that exceed token limit
            valid_texts = []
            valid_indices = []
            
            for j, text in enumerate(batch):
                original_index = batch_indices[j]
                token_count = self.estimate_token_count(text)
                
                if token_count > self.max_tokens:
                    logger.warning(f"Document at index {original_index} exceeds token limit ({token_count} > {self.max_tokens})")
                    skipped_indices.append(original_index)
                    
                    # Add to oversized document IDs if we have document IDs
                    if has_doc_ids:
                        doc_id = document_ids[original_index]
                        self.oversized_document_ids.add(doc_id)
                        logger.warning(f"Added document ID {doc_id} to oversized documents list (token count: {token_count})")
                else:
                    valid_texts.append(text)
                    valid_indices.append(original_index)
            
            # Skip this batch if all texts exceed token limit
            if not valid_texts:
                continue
            
            try:
                # Get embeddings for valid texts
                batch_embeddings = await self.get_embeddings_batch(valid_texts)
                
                # Add embeddings to results at the correct indices
                for embedding_idx, original_idx in enumerate(valid_indices):
                    # Extend results list if needed
                    while len(results) <= original_idx:
                        results.append(None)
                    
                    results[original_idx] = batch_embeddings[embedding_idx]
            except Exception as e:
                logger.error(f"Error processing batch: {str(e)}")
                # Mark all indices in this batch as skipped
                skipped_indices.extend(valid_indices)
        
        # Remove None values from results (these are skipped documents)
        results = [r for r in results if r is not None]
        
        return results, skipped_indices
    
    async def save_oversized_document_ids(self, output_path: str) -> None:
        """
        Save the IDs of documents that exceed the token limit to a file.
        
        Args:
            output_path: Path to save the file
        """
        if not self.oversized_document_ids:
            logger.info("No oversized document IDs to save")
            return
        
        try:
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            # Write IDs to file
            with open(output_path, "w", encoding="utf-8") as f:
                for doc_id in sorted(self.oversized_document_ids):
                    f.write(f"{doc_id}\n")
            
            logger.info(f"Saved {len(self.oversized_document_ids)} oversized document IDs to {output_path}")
        except Exception as e:
            logger.error(f"Error saving oversized document IDs: {str(e)}") 