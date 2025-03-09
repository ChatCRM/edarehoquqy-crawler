import os
import asyncio
from aiopath import AsyncPath
from typing import List, Dict, Any, AsyncGenerator, Tuple
import logging

logger = logging.getLogger(__name__)


class FileManager:
    """Manager for reading HTML files from the output directory."""
    
    def __init__(
        self,
        output_dir: str = "../output",
        max_concurrent_reads: int = 10,
    ):
        """
        Initialize the file manager.
        
        Args:
            output_dir: Path to the output directory
            max_concurrent_reads: Maximum number of concurrent file reads
        """
        self.output_dir = AsyncPath(output_dir).resolve()
        self.semaphore = asyncio.Semaphore(max_concurrent_reads)
    
    async def list_directories(self) -> List[Tuple[str, AsyncPath]]:
        """
        List all directories in the output directory.
        
        Returns:
            List[Tuple[str, AsyncPath]]: List of (directory_id, directory_path) tuples
        """
        if not await self.output_dir.exists():
            logger.error(f"Output directory {self.output_dir} does not exist")
            return []
        
        directories = []
        
        # Use async iteration instead of run_in_executor
        async for item in self.output_dir.iterdir():
            if await item.is_dir():
                dir_id = item.name
                directories.append((dir_id, item))
        
        logger.info(f"Found {len(directories)} directories in {self.output_dir}")
        return directories
    
    async def read_html_file(self, dir_id: str, dir_path: AsyncPath) -> Tuple[str, str, str]:
        """
        Read an HTML file from a directory.
        
        Args:
            dir_id: ID of the directory
            dir_path: Path to the directory
            
        Returns:
            Tuple[str, str, str]: (file_id, file_path, html_content)
            
        Raises:
            FileNotFoundError: If the HTML file does not exist
        """
        async with self.semaphore:
            file_id = dir_id
            file_path = dir_path / f"{file_id}.html"
            
            if not await file_path.exists():
                raise FileNotFoundError(f"HTML file {file_path} does not exist")
            
            # Use async file reading
            async with await file_path.open("r", encoding="utf-8") as f:
                content = await f.read()
            
            return file_id, str(file_path), content
    
    async def scan_directories(self) -> AsyncGenerator[Tuple[str, str, str], None]:
        """
        Scan directories and yield HTML file contents.
        
        Yields:
            Tuple[str, str, str]: (file_id, file_path, html_content)
        """
        directories = await self.list_directories()
        
        for dir_id, dir_path in directories:
            try:
                result = await self.read_html_file(dir_id, dir_path)
                yield result
            except Exception as e:
                logger.error(f"Error reading HTML file from directory {dir_id}: {str(e)}")
                continue
