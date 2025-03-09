import os
import asyncio
from aiopath import AsyncPath
from typing import List, Dict, Any, AsyncGenerator, Tuple
import logging
import aiofiles
import subprocess

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
        # Just create the AsyncPath without resolving it yet
        self.output_dir = AsyncPath(output_dir)
        self.semaphore = asyncio.Semaphore(max_concurrent_reads)
        self._initialized = False
    
    async def initialize(self) -> None:
        """
        Initialize the file manager asynchronously.
        This method should be called before using any other methods.
        """
        if not self._initialized:
            # Resolve the path asynchronously
            self.output_dir = await self.output_dir.resolve()
            self._initialized = True
    
    async def list_directories(self) -> List[Tuple[str, AsyncPath]]:
        """
        List all directories in the output directory, including hidden directories.
        Uses the find command to ensure all directories are found.
        
        Returns:
            List[Tuple[str, AsyncPath]]: List of (directory_id, directory_path) tuples
        """
        # Ensure the file manager is initialized
        await self.initialize()
        
        if not await self.output_dir.exists():
            logger.error(f"Output directory {self.output_dir} does not exist")
            return []
        
        directories = []
        
        # Use the find command to list all directories including hidden ones
        loop = asyncio.get_event_loop()
        try:
            # Use find with -a to include hidden files/directories
            cmd = f"find {self.output_dir} -mindepth 1 -maxdepth 1 -type d -print"
            result = await loop.run_in_executor(None, lambda: subprocess.check_output(cmd, shell=True, text=True))
            
            # Parse the result and create directory tuples
            for line in result.splitlines():
                if line.strip():
                    # Extract the directory name from the full path
                    dir_path = AsyncPath(line.strip())
                    dir_id = dir_path.name
                    directories.append((dir_id, dir_path))
                
            logger.info(f"Found {len(directories)} directories using find command")
        except Exception as e:
            logger.error(f"Error running find command to find directories: {str(e)}")
            
            # Fallback to the original method if the shell command fails
            logger.info("Falling back to iterdir() method")
            async for item in self.output_dir.iterdir():
                try:
                    is_dir = await item.is_dir()
                    if is_dir:
                        dir_id = item.name
                        directories.append((dir_id, item))
                except (PermissionError, OSError) as e:
                    logger.warning(f"Error accessing {item}: {str(e)}")
            
            logger.info(f"Found {len(directories)} directories using iterdir() method")
        
        # Let's also try the ls -la approach as a verification
        try:
            cmd = f"ls -la {self.output_dir} | grep '^d' | grep -v '\\.$' | wc -l"
            result = await loop.run_in_executor(None, lambda: subprocess.check_output(cmd, shell=True, text=True))
            ls_count = int(result.strip())
            logger.info(f"ls -la reports {ls_count} directories (excluding . and ..)")
            
            # If ls -la finds more directories, try to get them
            if ls_count > len(directories):
                logger.warning(f"ls -la found {ls_count} directories but find command found {len(directories)}. Trying to get the missing directories.")
                cmd = f"ls -la {self.output_dir} | grep '^d' | awk '{{print $NF}}'"
                result = await loop.run_in_executor(None, lambda: subprocess.check_output(cmd, shell=True, text=True))
                
                # Get current directory names
                current_dir_names = {dir_id for dir_id, _ in directories}
                
                # Add any missing directories
                for line in result.splitlines():
                    dir_name = line.strip()
                    if dir_name and dir_name not in ['.', '..'] and dir_name not in current_dir_names:
                        dir_path = self.output_dir / dir_name
                        directories.append((dir_name, dir_path))
                        logger.info(f"Added missing directory: {dir_name}")
        except Exception as e:
            logger.error(f"Error running ls verification: {str(e)}")
        
        logger.info(f"Total: Found {len(directories)} directories in {self.output_dir}")
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
        # Ensure the file manager is initialized
        await self.initialize()
        
        async with self.semaphore:
            file_id = dir_id
            file_path = dir_path / f"{file_id}.html"
            
            if not await file_path.exists():
                raise FileNotFoundError(f"HTML file {file_path} does not exist")
            
            # Use async file reading
            async with aiofiles.open(file_path, mode="r", encoding="utf-8") as f:
                content = await f.read()
            
            return file_id, str(file_path), content
    
    async def scan_directories(self) -> AsyncGenerator[Tuple[str, str, str], None]:
        """
        Scan directories and yield HTML file contents.
        
        Yields:
            Tuple[str, str, str]: (file_id, file_path, html_content)
        """
        directories = await self.list_directories()
        logger.info(f"Scanning {len(directories)} directories for HTML files")
        
        processed_count = 0
        error_count = 0
        
        for dir_id, dir_path in directories:
            try:
                result = await self.read_html_file(dir_id, dir_path)
                processed_count += 1
                if processed_count % 1000 == 0:
                    logger.info(f"Processed {processed_count} directories so far")
                yield result
            except FileNotFoundError:
                # This is expected if there's no HTML file in the directory
                logger.debug(f"No HTML file found in directory {dir_id}")
                error_count += 1
            except Exception as e:
                logger.error(f"Error reading HTML file from directory {dir_id}: {str(e)}")
                error_count += 1
        
        logger.info(f"Completed scanning {len(directories)} directories. Processed: {processed_count}, Errors: {error_count}")
