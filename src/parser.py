from pydantic import field_validator, BaseModel, Field
from typing import Dict, Any, Optional, List, Set
from datetime import datetime
import logging
from lxml import html
from hazm import Normalizer
import time
import jdatetime
import re

logger = logging.getLogger(__name__)

class DateFormat(BaseModel):
    gregorian: str = "0001/01/01"
    shamsi: str = "0001/01/01"

class Metadata(BaseModel):
    file_id: str
    file_number: str = "نامشخص"
    opinion_number: str = "نامشخص"
    opinion_date: DateFormat = Field(default_factory=DateFormat)
    
    @field_validator('opinion_date', mode='before')
    def validate_opinion_date(cls, v):
        if v is None:
            return DateFormat()
        
        if isinstance(v, str):
            # Try to extract date from string
            date_pattern = re.search(r'(\d{4})[/\\-](\d{1,2})[/\\-](\d{1,2})', v)
            if date_pattern:
                year, month, day = date_pattern.groups()
                shamsi_date = f"{year}/{month}/{day}"
                date_formats = DateHandler.process_date(shamsi_date)
                return DateFormat(
                    gregorian=date_formats["gregorian"],
                    shamsi=date_formats["shamsi"]
                )
            return DateFormat()
        
        if isinstance(v, dict):
            return DateFormat(**v)
        
        return v
    
    @field_validator('file_number', 'opinion_number', mode='before')
    def clean_metadata_fields(cls, v):
        if v is None:
            return "نامشخص"
        
        if isinstance(v, str):
            # Clean up the value - keep only numbers, slashes, and dashes
            cleaned_value = ''.join(c for c in v if c.isdigit() or c in '-/کط')
            return cleaned_value if cleaned_value else "نامشخص"
        
        return v


class ParsedContent(BaseModel):
    """Model representing parsed content from HTML files."""
    question: str
    answer: str
    metadata: Metadata = Field(default_factory=Metadata)
    content: str  

    @field_validator('content', 'question', 'answer', mode='before')
    def remove_newlines(cls, v):
        if v is None:
            return ""
            
        if not isinstance(v, str):
            return str(v)
            
        try:
            normalizer = Normalizer()
            v = normalizer.normalize(v)
        except Exception as e:
            logger.error(f"Error normalizing text: {str(e)}")
            
        # Replace specific phrases with line breaks for better readability
        v = v.replace('نظریه مشورتی اداره کل حقوقی قوه قضاییه :', '\n\n')
        
        # Remove extra whitespace
        v = re.sub(r'\s+', ' ', v).strip()
        
        return v


class DateHandler:
    @classmethod
    def is_valid_shamsi_date(cls, year: int, month: int, day: int) -> bool:
        """Check if the given Shamsi date is valid."""
        try:
            jdatetime.date(year, month, day)
            return True
        except (ValueError, TypeError):
            logger.error(f"Invalid Shamsi date: {year}/{month}/{day}")
            return False

    @classmethod
    def process_date(cls, value: str) -> Dict[str, str]:
        """Process a date string and convert it to Shamsi and Gregorian dates."""
        if not value or not isinstance(value, str):
            return {"shamsi": "0001/01/01", "gregorian": "0001/01/01"}

        # Normalize the date separator
        normalized_date = re.sub(r'[/\\-]', '/', value.strip())
        
        # Remove any non-digit and non-separator characters
        normalized_date = re.sub(r'[^\d/]', '', normalized_date)

        # Handle all-zero dates and invalid formats
        if normalized_date in ['0000/00/00', '0/0/0', '00/00/00', '0', '00', '', ' ']:
            return {"shamsi": "0001/01/01", "gregorian": "0001/01/01"}

        # Split the date parts
        parts = normalized_date.split('/')

        # If we don't have exactly 3 parts, return default dates
        if len(parts) != 3:
            return {"shamsi": "0001/01/01", "gregorian": "0001/01/01"}

        try:
            year, month, day = [int(p) for p in parts]
        except (ValueError, TypeError):
            return {"shamsi": "0001/01/01", "gregorian": "0001/01/01"}
            
        # Ensure year is 4 digits
        if year < 100:
            year += 1400  # Assume it's a short form of a year in the 1400s (Shamsi)
            
        # Validate the Shamsi date
        if not cls.is_valid_shamsi_date(year, month, day):
            return {"shamsi": f"{year:04d}/{month:02d}/{day:02d}", "gregorian": "0001/01/01"}

        # Format the Shamsi date
        normalized_shamsi = f"{year:04d}/{month:02d}/{day:02d}"

        # Convert to Gregorian
        try:
            j_date = jdatetime.date(year, month, day)
            g_date = j_date.togregorian()
            gregorian_date = f"{g_date.year:04d}/{g_date.month:02d}/{g_date.day:02d}"
            
            # Handle edge cases for Gregorian date
            if gregorian_date in ['0000/00/00', '0/0/0', '00/00/00', '0', '00', '']:
                gregorian_date = "0001/01/01"
        except (ValueError, TypeError) as e:
            logger.error(f"Error converting date {normalized_shamsi} to Gregorian: {str(e)}")
            return {"shamsi": normalized_shamsi, "gregorian": "0001/01/01"}
            
        return {"shamsi": normalized_shamsi, "gregorian": gregorian_date}

    @classmethod
    def process_value(cls, value: Any, skip_keys: Optional[Set[str]] = None) -> Any:
        if skip_keys is None:
            skip_keys = set()

        if isinstance(value, str):
            # Skip processing if the key is in skip_keys
            if value in skip_keys:
                logger.info(f"Value skipped because key '{value}' is in skip_keys")
                return value

            # Check if the value looks like a date
            if re.search(r'\b\d{2,4}[/\\]\d{1,2}[/\\]\d{1,2}\b', value):
                processed_date = cls.process_date(value.strip())
                return {
                    "gregorian": processed_date["gregorian"],
                    "shamsi": processed_date["shamsi"]
                }
            return value
        elif isinstance(value, dict):
            return {k: cls.process_value(v, skip_keys) for k, v in value.items()}
        elif isinstance(value, list):
            return [cls.process_value(item, skip_keys) for item in value]
        return value


class HTMLParser:
    """Parser for HTML files."""
    def parse(self, html_content: str, file_id: str) -> ParsedContent:
        """
        Parse HTML content and extract structured data.
        
        Args:
            html_content: The HTML content to parse
            file_id: The ID of the file being parsed
            
        Returns:
            ParsedContent: Structured data extracted from the HTML
        """
        start_time = time.time()
        
        try:
            # Parse HTML with lxml
            tree = html.fromstring(html_content)
            
            # Extract metadata
            try:
                metadata = self._extract_metadata(tree, file_id)
            except Exception as e:
                logger.error(f"Error extracting metadata for file {file_id}: {str(e)}")
                metadata = Metadata(file_id=file_id)
            
            # Extract question
            try:
                question = self._extract_question(tree)
                if not question:
                    question = "سوال نامشخص"
            except Exception as e:
                logger.error(f"Error extracting question for file {file_id}: {str(e)}")
                question = "سوال نامشخص"
            
            # Extract answer
            try:
                answer = self._extract_answer(tree)
                if not answer:
                    answer = "پاسخ نامشخص"
            except Exception as e:
                logger.error(f"Error extracting answer for file {file_id}: {str(e)}")
                answer = "پاسخ نامشخص"
            
            # Extract content for embedding
            try:
                content = self._extract_content_section(tree)
                if not content:
                    content = f"{question} {answer}"
            except Exception as e:
                logger.error(f"Error extracting content for file {file_id}: {str(e)}")
                content = f"{question} {answer}"
            
            # Create ParsedContent object
            parsed_content = ParsedContent(
                question=question,
                answer=answer,
                metadata=metadata,
                content=content
            )
            
            processing_time = time.time() - start_time
            logger.info(f"Parsed file {file_id} in {processing_time:.2f} seconds")
            
            return parsed_content
        except Exception as e:
            logger.error(f"Error parsing HTML content for file {file_id}: {str(e)}")
            # Return a minimal valid ParsedContent object
            return ParsedContent(
                question="سوال نامشخص",
                answer="پاسخ نامشخص",
                metadata=Metadata(file_id=file_id),
                content="محتوای نامشخص"
            )
    
    def _extract_metadata(self, tree: html.HtmlElement, file_id: str) -> Metadata:
        """
        Extract metadata from HTML.
        
        Args:
            tree: lxml HTML Element
            file_id: ID of the file
            
        Returns:
            Metadata: Extracted metadata
        """
        metadata = {'file_id': file_id}
        
        # Define XPaths and their corresponding metadata keys
        xpath_mappings = [
            ('//*[@id="mvcContainer-1286"]/div/div/div[2]/div/div[1]/div[2]/div[2]', 'file_number'),  # شماره پرونده
            ('//*[@id="mvcContainer-1286"]/div/div/div[2]/div/div[1]/div[2]/div[1]', 'opinion_number'),  # شماره نظریه
            ('//*[@id="mvcContainer-1286"]/div/div/div[2]/div/div[1]/div[2]/div[3]', 'opinion_date')  # تاریخ نظریه
        ]
        
        # Extract text from each XPath
        for xpath, key in xpath_mappings:
            try:
                value = self._extract_content(tree, xpath)
                if value:
                    # Clean up the value - keep only numbers, slashes, and dashes
                    if key in ['file_number', 'opinion_number']:
                        # Extract only numbers and dashes for file/opinion numbers
                        cleaned_value = ''.join(c for c in value if c.isdigit() or c in '-/کط')
                        metadata[key] = cleaned_value if cleaned_value else "نامشخص"
                    else:
                        metadata[key] = value
                else:
                    metadata[key] = "نامشخص" if key != 'opinion_date' else None
            except Exception as e:
                metadata[key] = "نامشخص" if key != 'opinion_date' else None
                logger.error(f"Error extracting {key} using XPath {xpath} from file {file_id}: {str(e)}")
        
        # Process the opinion_date to create a DateFormat object
        if 'opinion_date' in metadata and metadata['opinion_date']:
            try:
                # Extract date pattern from the text (looking for YYYY/MM/DD format)
                date_text = metadata['opinion_date']
                date_pattern = re.search(r'(\d{4})[/\\-](\d{1,2})[/\\-](\d{1,2})', date_text)
                
                if date_pattern:
                    # Extract the date components
                    year, month, day = date_pattern.groups()
                    shamsi_date = f"{year}/{month}/{day}"
                    
                    # Use DateHandler to process the date
                    date_formats = DateHandler.process_date(shamsi_date)
                    
                    metadata['opinion_date'] = DateFormat(
                        gregorian=date_formats["gregorian"],
                        shamsi=date_formats["shamsi"]
                    )
                else:
                    # If no date pattern found, use default
                    metadata['opinion_date'] = DateFormat()
            except Exception as e:
                logger.error(f"Error processing date: {str(e)}")
                metadata['opinion_date'] = DateFormat()
        else:
            metadata['opinion_date'] = DateFormat()

        return Metadata(**metadata)
    
    def _extract_content(self, tree: html.HtmlElement, xpath: str) -> Optional[str]:
        """
        Extract content from HTML.
        
        Args:
            tree: lxml HTML Element
            xpath: XPath expression to extract content

        Returns:
            Optional[str]: Extracted content
        """
        content = tree.xpath(xpath)
        if not content or len(content) == 0:
            return None
        return content[0].text_content().strip().replace('\n', ' ')

    def _extract_content_section(self, tree: html.HtmlElement) -> Optional[str]:
        """
        Extract content section from HTML.
        
        Args:
            tree: lxml HTML Element
        """
        return self._extract_content(tree, '//*[@id="mvcContainer-1286"]/div/div/div[2]/div/div[2]')
    
    def _extract_question(self, tree: html.HtmlElement) -> Optional[str]:
        """
        Extract question from HTML.
        
        Args:
            tree: lxml HTML Element
        """
        return self._extract_content(tree, '//*[@id="mvcContainer-1286"]/div/div/div[2]/div/div[2]/div/div[2]/div')
    
    def _extract_answer(self, tree: html.HtmlElement) -> Optional[str]:
        """
        Extract answer from HTML.
        
        Args:
            tree: lxml HTML Element
        """
        return self._extract_content(tree, '//*[@id="mvcContainer-1286"]/div/div/div[2]/div/div[2]/div/div[4]/div')


if __name__ == "__main__":
    start = time.time()
    parser = HTMLParser()
    with open("test.html", "r") as file:
        html_content = file.read()
    parsed_content = parser.parse(html_content, "test")
    print(parsed_content)
    with open("parsed_content.json", "w") as file:
        file.write(parsed_content.model_dump_json(indent=4))
    end = time.time()
    print(f"Time taken: {end - start} seconds")

