import os
import asyncio
import logging
from typing import List, Dict, Any, Optional
import time
from openai import AsyncOpenAI
from openai.types.create_embedding_response import CreateEmbeddingResponse
import random

logger = logging.getLogger(__name__)


class EmbeddingService:
    """Service for generating embeddings using OpenAI's API."""
    
    def __init__(
        self,
        api_key: str = None,
        model: str = "text-embedding-3-large",
        max_retries: int = 4,
        rate_limit: int = 60,  # Requests per minute
        timeout: int = 30,
        batch_size: int = 10,  # Number of texts to batch in a single API call
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
        """
        self.api_key = api_key or os.environ.get("OPENAI_API_KEY")
        if not self.api_key:
            raise ValueError("OpenAI API key is required")
        
        self.model = model
        self.max_retries = max_retries
        self.timeout = timeout
        self.batch_size = batch_size
        
        # Initialize OpenAI client
        self.client = AsyncOpenAI(
            base_url="https://api.avalai.ir/v1",
            api_key=self.api_key,
            timeout=timeout
        )
        
        # Rate limiting
        self.semaphore = asyncio.Semaphore(rate_limit // 6 + 1)  # Allow bursts but average to rate limit
        self.rate_limit_period = 60 / rate_limit  # Time between requests in seconds
        self.last_request_time = 0
    
    async def get_embedding(self, text: str) -> List[float]:
        """
        Get embedding for a single text.
        
        Args:
            text: The text to embed
            
        Returns:
            List[float]: The embedding vector
        """
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
    
    async def process_texts_in_batches(self, texts: List[str]) -> List[List[float]]:
        """
        Process a list of texts in batches to get embeddings.
        
        Args:
            texts: List of texts to embed
            
        Returns:
            List[List[float]]: List of embedding vectors
        """
        if not texts:
            return []
        
        results = []
        
        # Process in batches
        for i in range(0, len(texts), self.batch_size):
            batch = texts[i:i + self.batch_size]
            batch_embeddings = await self.get_embeddings_batch(batch)
            results.extend(batch_embeddings)
        
        return results 