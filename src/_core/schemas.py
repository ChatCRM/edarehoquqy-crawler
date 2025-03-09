from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any, Literal
from aiopath import AsyncPath


class CrawlerConfig(BaseModel):
    """Configuration for the crawler"""
    max_concurrent_requests: int = Field(default=5, gt=0)
    max_queue_size: int = Field(default=100, gt=0)
    request_delay: float = Field(default=0.5, ge=0)
    timeout: int = Field(default=15, gt=0)
    base_path: AsyncPath = Field(default=AsyncPath("output"))


class IdeaPageInfo(BaseModel):
    """Information about an idea page"""
    idea_id: str
    document_url: str
    page_number: int
    raw_data: Dict[str, Any]


class CustomSearchParams(BaseModel):
    """Parameters for search requests"""
    search: str = ""
    pageIndex: int = Field(default=1, gt=0)
    pageSize: Literal[10, 20, 30] = 10
    sortOption: Literal[0, 1] = 1
    culture: str = "fa-IR"
    fromDate: str = ""
    toDate: str = ""
    moduleId: int = 1286


class ResultAttributes(BaseModel):
    """Attributes for a search result item"""
    # Empty model as the Attributes field appears to be an empty object in the example


class ResultItem(BaseModel):
    """Individual result item within a search result"""
    IdeaId: Optional[int] = None
    DocumentUrl: str
    Title: str
    Description: Optional[str] = None
    AuthorName: Optional[str] = None
    DisplayModifiedTime: Optional[str] = None
    raw_html: Optional[str] = None


class SearchResult(BaseModel):
    """Single search result entry"""

    DocumentUrl: str
    Title: str
    Results: List[ResultItem]


class SearchResponse(BaseModel):
    """Response model for the custom search API"""
    results: List[ResultItem]
    totalHits: int
    more: bool
    raw_html: Optional[str] = None
