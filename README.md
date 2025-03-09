# HTML Processing and Indexing System

This system processes HTML files from a directory structure, parses them, generates embeddings, and indexes them in Elasticsearch. It uses asynchronous processing with queues and workers to efficiently handle large numbers of files.

## Features

- Asynchronous processing with queues and workers
- Rate limiting and concurrency control
- Error handling and retries
- Configurable number of workers and queue sizes
- Elasticsearch integration with vector search support
- OpenAI embeddings integration

## Requirements

- Python 3.12 or higher
- Elasticsearch 8.x
- OpenAI API key

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/edarehoquqy-crawler.git
   cd edarehoquqy-crawler
   ```

2. Create a virtual environment and install dependencies:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   pip install -e .
   ```

3. Set up environment variables:
   ```bash
   cp .env.example .env
   ```
   
   Edit the `.env` file to include your OpenAI API key, Elasticsearch credentials, and other configuration options:
   ```
   # Output directory
   OUTPUT_DIR=../output

   # Worker configuration
   NUM_PARSER_WORKERS=5
   NUM_EMBEDDING_WORKERS=3
   NUM_INDEXING_WORKERS=5

   # Queue sizes
   PARSER_QUEUE_SIZE=100
   EMBEDDING_QUEUE_SIZE=100
   INDEXING_QUEUE_SIZE=100

   # Elasticsearch configuration
   ELASTICSEARCH_INDEX=edarehoquqy
   ELASTICSEARCH_HOST=http://localhost:9200
   ELASTICSEARCH_USERNAME=elastic
   ELASTICSEARCH_PASSWORD=your_password

   # OpenAI API configuration for embeddings
   OPENAI_API_KEY=your_openai_api_key

   # Optional metadata file path
   METADATA_FILE=
   ```

## Usage

Run the processor with settings from your .env file:

```bash
python -m src.main
```

The application will automatically load configuration from the .env file. You can override any setting by modifying the .env file.

### Environment Variables

All configuration can be set through environment variables:

- `OUTPUT_DIR`: Path to the output directory (default: "../output")
- `NUM_PARSER_WORKERS`: Number of parser workers (default: 5)
- `NUM_EMBEDDING_WORKERS`: Number of embedding workers (default: 3)
- `NUM_INDEXING_WORKERS`: Number of indexing workers (default: 5)
- `PARSER_QUEUE_SIZE`: Size of the parser queue (default: 100)
- `EMBEDDING_QUEUE_SIZE`: Size of the embedding queue (default: 100)
- `INDEXING_QUEUE_SIZE`: Size of the indexing queue (default: 100)
- `ELASTICSEARCH_INDEX`: Name of the Elasticsearch index (default: "edarehoquqy")
- `ELASTICSEARCH_HOST`: Elasticsearch host URL (default: "http://localhost:9200")
- `ELASTICSEARCH_USERNAME`: Elasticsearch username (optional)
- `ELASTICSEARCH_PASSWORD`: Elasticsearch password (optional)
- `OPENAI_API_KEY`: OpenAI API key for generating embeddings
- `METADATA_FILE`: Path to a JSON file containing metadata (optional)

## Directory Structure

The system expects the following directory structure:

```
output/
  ├── id1/
  │   └── id1.html
  ├── id2/
  │   └── id2.html
  └── ...
```

Where each directory is named with an ID, and contains an HTML file with the same name.

## Customizing the Parser

The current implementation includes a placeholder parser. To customize it, edit the `HTMLParser` class in `src/parser.py` to implement your specific parsing logic.

## Metadata

You can provide additional metadata to be included in the indexed documents by passing a JSON file with the `--metadata-file` argument. The metadata will be merged with the document metadata.

Example metadata.json:

```json
{
  "source": "edarehoquqy",
  "language": "fa",
  "collection": "legal_opinions"
}
```

## Elasticsearch Index

The system creates an Elasticsearch index with the following mapping:

```json
{
  "mappings": {
    "properties": {
      "question": {"type": "text"},
      "answer": {"type": "text"},
      "date": {"type": "date"},
      "metadata": {"type": "object"},
      "embedding": {
        "type": "dense_vector",
        "dims": 3072,
        "index": true,
        "similarity": "cosine"
      }
    }
  }
}
```

This mapping supports vector search using the `embedding` field.

## License

This project is licensed under the terms of the license included in the repository.
