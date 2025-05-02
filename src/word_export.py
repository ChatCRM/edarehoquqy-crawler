import aiofiles
import asyncio
import json
from aiopath import AsyncPath
from typing import List, Dict, Tuple, Optional, AsyncGenerator, Set
from docx import Document
from docxcompose.composer import Composer
from docx.shared import Inches, Pt, RGBColor
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.enum.style import WD_STYLE_TYPE
import logging

from src.html_converter import get_all_existing_ids, EdarehoquqyDocument

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FileLoader:
    def __init__(self, root_file_path: str):
        self.root_file_path = AsyncPath(root_file_path)
        self._all_existing_ids = dict()
        self._failed_files = set()
        self._failed_ids: Set[Tuple[str, str]] = set()
        
    async def get_path_ids(self, file_path: AsyncPath) -> None:
        if not await file_path.exists():
            return None
        self._all_existing_ids[file_path.name] = await file_path.glob('*')
        return None

    async def _load_object(self, file_path: AsyncPath) -> AsyncGenerator[EdarehoquqyDocument, None]:
        for _file_path in self._all_existing_ids[file_path.name]:
            async with aiofiles.open(_file_path, mode='r', encoding='utf-8') as f:
                data = await f.read()
                if data := json.loads(data, object_hook=lambda d: EdarehoquqyDocument(**d)):
                    yield data
                else:
                    logger.error(f"Failed to load object from {_file_path}")
                    self._failed_files.add(file_path.name)
                    self._failed_ids.add(_file_path.name)
                    continue

    async def get__data(self) -> AsyncGenerator[EdarehoquqyDocument, None]:
        await self.get_path_ids(self.root_file_path)
        for file_path in self._all_existing_ids.keys():
            if data := await self._load_object(file_path):
                yield data
            
    

class WordExporter:
    def __init__(self, file_loader: FileLoader):
        self.file_loader = file_loader
    
    def _setup_styles(self, doc: Document) -> None:
        # Title style
        title_style = doc.styles.add_style('CustomTitle', WD_STYLE_TYPE.PARAGRAPH)
        title_font = title_style.font
        title_font.size = Pt(16)
        title_font.bold = True
        title_font.color.rgb = RGBColor(0, 0, 0)
        
        # Metadata style
        meta_style = doc.styles.add_style('MetadataLabel', WD_STYLE_TYPE.PARAGRAPH)
        meta_font = meta_style.font
        meta_font.size = Pt(11)
        meta_font.bold = True
        meta_font.color.rgb = RGBColor(89, 89, 89)
        
        # Content style
        content_style = doc.styles.add_style('ContentText', WD_STYLE_TYPE.PARAGRAPH)
        content_font = content_style.font
        content_font.size = Pt(12)

    def _create_document(self, data: EdarehoquqyDocument) -> Document:
        doc = Document()
        self._setup_styles(doc)
        
        # Add title
        title = doc.add_heading(data.title, level=1)
        title.style = doc.styles['CustomTitle']
        title.alignment = WD_ALIGN_PARAGRAPH.CENTER
        
        # Add metadata section
        metadata_section = doc.add_paragraph(style='MetadataLabel')
        metadata_section.add_run('Document Metadata\n').bold = True
        
        # Format metadata nicely
        if hasattr(data, 'metadata'):
            for key, value in data.metadata.items():
                p = doc.add_paragraph(style='MetadataLabel')
                p.add_run(f"{key}: ").bold = True
                p.add_run(str(value))
        
        # Add separator
        doc.add_paragraph('_' * 50, style='ContentText')
        
        # Add main content
        if hasattr(data, 'content'):
            content_para = doc.add_paragraph(style='ContentText')
            content_para.add_run(data.content)
            
        # Add any additional fields
        if hasattr(data, 'references'):
            doc.add_heading('References', level=2)
            for ref in data.references:
                p = doc.add_paragraph(style='ContentText')
                p.add_run(f"• {ref}")
                
        # Add page break at the end
        doc.add_page_break()
        
        return doc 
    
    async def export_to_word(self) -> AsyncGenerator[Document, None]:
        async for data in self.file_loader.get_json_data():
            yield self._create_document(data)


    
    

