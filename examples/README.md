# HTML Converter Examples

This directory contains examples and sample files for using the HTML converter.

## Using the HTML Converter

The HTML converter can be used in two ways:

1. **With Elasticsearch queries** - Retrieves documents from Elasticsearch and converts them to HTML
2. **With direct JSON files** - Converts JSON files directly to HTML without needing Elasticsearch

### 1. Converting from Elasticsearch

```bash
# Basic usage - search for documents with keyword "سرقت"
python -m src.html_converter "سرقت" -o ./html-output

# Search for multiple terms
python -m src.html_converter "سرقت" "تصرف عدوانی" -o ./html-output

# Generate titles and summaries using OpenAI
python -m src.html_converter "سرقت" -o ./html-output -t
```

### 2. Converting from JSON files

If you're having issues with Elasticsearch or want to convert local files, you can use the `-f` option:

```bash
# Convert a single JSON file
python -m src.html_converter -f examples/sample_document.json -o ./html-output

# Convert multiple JSON files
python -m src.html_converter -f examples/sample_document.json examples/another_document.json -o ./html-output

# Generate titles and summaries for JSON files that don't have them
python -m src.html_converter -f examples/sample_document.json -o ./html-output -t
```

### 3. Combining both approaches

You can even combine Elasticsearch queries with direct file conversion:

```bash
# Convert both Elasticsearch results and JSON files
python -m src.html_converter "سرقت" -f examples/sample_document.json -o ./html-output
```

## Sample Document Format

JSON files should follow this format:

```json
{
    "id_edarehoquqy": "DOCUMENT_ID",
    "question": "سوال حقوقی",
    "answer": "پاسخ حقوقی",
    "summary": "خلاصه سند (اختیاری)",
    "title": "عنوان سند (اختیاری)",
    "metdata": {
        "file_id": "FILE_ID",
        "file_number": "123/456",
        "opinion_date": {
            "shamsi": "1402/05/12",
            "gregorian": "2023/08/03"
        },
        "opinion_number": "789/ABC"
    }
}
```

The `summary` and `title` fields are optional. If you use the `-t` option, they will be generated automatically if missing.

## Troubleshooting

If you encounter connection issues with Elasticsearch, try these solutions:

1. **Check your environment variables:**
   - Ensure `ELASTICSEARCH_HOST`, `ELASTICSEARCH_USERNAME`, and `ELASTICSEARCH_PASSWORD` are correctly set

2. **Use direct file conversion:**
   - Use the `-f` option to convert JSON files directly without Elasticsearch

3. **Check network connectivity:**
   - Make sure your network allows connections to the Elasticsearch server
   - Try using a VPN if you're behind a restrictive firewall

4. **Test Elasticsearch separately:**
   - Use a tool like curl to test your Elasticsearch connection independently 