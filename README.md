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
   cp .env.sample .env
   ```
   
   Edit the `.env` file to include your OpenAI API key and Elasticsearch credentials:
   ```
   OPENAI_API_KEY=your_openai_api_key
   ELASTICSEARCH_HOST=http://localhost:9200
   ELASTICSEARCH_USERNAME=elastic
   ELASTICSEARCH_PASSWORD=your_password
   ```

## Usage

Run the processor with default settings:

```bash
python src/main.py
```

### Command-line Arguments

- `--output-dir`: Path to the output directory (default: "../output")
- `--num-parser-workers`: Number of parser workers (default: 5)
- `--num-embedding-workers`: Number of embedding workers (default: 3)
- `--num-indexing-workers`: Number of indexing workers (default: 5)
- `--parser-queue-size`: Size of the parser queue (default: 100)
- `--embedding-queue-size`: Size of the embedding queue (default: 100)
- `--indexing-queue-size`: Size of the indexing queue (default: 100)
- `--elasticsearch-index`: Name of the Elasticsearch index (default: "parsed_content")
- `--metadata-file`: Path to a JSON file containing metadata

Example:

```bash
python src/main.py --output-dir /path/to/output --num-parser-workers 10 --elasticsearch-index my_index --metadata-file metadata.json
```

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
