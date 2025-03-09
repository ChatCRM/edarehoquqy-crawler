import os
import asyncio
import logging
from dotenv import load_dotenv
from elasticsearch import AsyncElasticsearch

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

async def test_elasticsearch_connection():
    """Test the Elasticsearch connection using both sets of credentials."""
    
    # Test with ES_URL and ES_PASSWORD
    es_url = os.environ.get("ES_URL")
    es_password = os.environ.get("ES_PASSWORD")
    
    if es_url and es_password:
        logger.info(f"Testing connection to {es_url} with ES_PASSWORD")
        try:
            client1 = AsyncElasticsearch(
                [es_url],
                basic_auth=("elastic", es_password)
            )
            info = await client1.info()
            logger.info(f"Successfully connected to Elasticsearch using ES_URL: {info}")
            await client1.close()
        except Exception as e:
            logger.error(f"Error connecting to Elasticsearch using ES_URL: {str(e)}")
    
    # Test with ELASTICSEARCH_HOST, ELASTICSEARCH_USERNAME, and ELASTICSEARCH_PASSWORD
    es_host = os.environ.get("ELASTICSEARCH_HOST")
    es_username = os.environ.get("ELASTICSEARCH_USERNAME")
    es_password = os.environ.get("ELASTICSEARCH_PASSWORD")
    
    if es_host and es_username and es_password:
        logger.info(f"Testing connection to {es_host} with ELASTICSEARCH_USERNAME and ELASTICSEARCH_PASSWORD")
        try:
            client2 = AsyncElasticsearch(
                [es_host],
                basic_auth=(es_username, es_password)
            )
            info = await client2.info()
            logger.info(f"Successfully connected to Elasticsearch using ELASTICSEARCH_HOST: {info}")
            await client2.close()
        except Exception as e:
            logger.error(f"Error connecting to Elasticsearch using ELASTICSEARCH_HOST: {str(e)}")

if __name__ == "__main__":
    asyncio.run(test_elasticsearch_connection()) 