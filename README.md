# EdarehoquqyDocument HTML Converter

This tool converts EdarehoquqyDocument objects to HTML files with a clean, Markdown-like style. The generated HTML files are right-to-left and optimized for Persian legal documents.

## Features

- Converts EdarehoquqyDocument objects to HTML files
- Support for batch conversion of multiple files
- Clean, readable HTML output with responsive design
- Right-to-left text direction for Persian content
- Maintains document structure (metadata, question, answer, summary)
- Automatic title generation for documents

## Usage

### From Python Code

```python
from pathlib import Path
from src.csv_export import EdarehoquqyDocument
from src.html_export import export_documents_to_html

# Create EdarehoquqyDocument objects
documents = [
    EdarehoquqyDocument(
        id_edarehoquqy="ABC123",
        question="سوال حقوقی نمونه",
        answer="پاسخ به سوال حقوقی",
        summary="خلاصه سند",
        title="عنوان نظریه حقوقی",
        metdata={
            "file_id": "FILE_001",
            "file_number": "789/XYZ",
            "opinion_date": {
                "shamsi": "1402/04/05",
                "gregorian": "2023/06/26"
            },
            "opinion_number": "123/456"
        }
    )
]

# Export to HTML
output_dir = Path("./html-output")
await export_documents_to_html(documents, output_dir)
```

### Command Line Tool

The package includes a command-line tool for converting JSON files to HTML:

```bash
# Convert a single file
python -m src.html_converter path/to/document.json -o output/directory

# Convert all JSON files in a directory
python -m src.html_converter path/to/documents/directory -b -o output/directory

# Generate titles for documents that don't have them
python -m src.html_converter path/to/document.json -o output/directory -t
```

Command-line options:

- `-o, --output-dir`: Output directory (default: ./html-output)
- `-b, --batch`: Batch mode - treat input as a directory and convert all JSON files
- `-p, --pattern`: File pattern to match in batch mode (default: *.json)
- `-t, --generate-titles`: Generate titles for documents that don't have them

## Sample JSON Format

The input JSON files should have the following structure:

```json
{
    "id_edarehoquqy": "ABC123",
    "question": "متن سوال حقوقی در اینجا",
    "answer": "متن پاسخ به سوال حقوقی در اینجا",
    "summary": "خلاصه نظر در اینجا",
    "title": "عنوان نظریه حقوقی",
    "metdata": {
        "file_id": "FILE_001",
        "file_number": "789/XYZ",
        "opinion_date": {
            "shamsi": "1402/04/05",
            "gregorian": "2023/06/26"
        },
        "opinion_number": "123/456"
    }
}
```

## Title Generation

If a document doesn't have a title, you can use the `-t` option to generate one automatically using OpenAI's API. The tool will:

1. Extract the question and answer from the document
2. Send them to the OpenAI API to generate a concise, descriptive title
3. Use the generated title in the HTML output

If you don't use the `-t` option and a document doesn't have a title, the tool will use the opinion number as the title.

## Generated HTML Structure

The generated HTML files follow a consistent structure:

1. Document title (from title field or generated)
2. Metadata section (document ID, file numbers, dates)
3. Summary section (if available)
4. Question section
5. Answer section
6. Footer with issue date

A sample HTML template is available in `templates/document_template.html`.

## Integration with Main Script

The HTML converter is integrated with the main CSV export script. When running the main script, it will:

1. Search for documents based on keywords
2. Generate text summaries 
3. Export to both text and HTML formats

## Requirements

- Python 3.7+
- aiofiles
- pathlib
- openai (for title generation)

## License

This project is licensed under the MIT License.
