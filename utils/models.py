"""
Data models for the crawl-news application.
"""

from pydantic import BaseModel, Field


class NewsData(BaseModel):
    title: str = Field(description="The title of the news")
    content: str = Field(description="The content of the news")
    url: str = Field(description="The URL of the news")
    date: str = Field(description="The date of the news") 