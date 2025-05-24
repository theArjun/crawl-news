"""
Crawler utility functions for the crawl-news application.
"""

import logging
from pathlib import Path
from crawl4ai import (
    AsyncWebCrawler,
    PruningContentFilter,
    DefaultMarkdownGenerator,
    CrawlerRunConfig,
    LLMExtractionStrategy,
    LLMConfig,
)

from .models import NewsData
from .url_utils import get_domain, get_url_fingerprint


def create_crawler_config() -> CrawlerRunConfig:
    """Creates and returns the crawler configuration."""
    prune_filter = PruningContentFilter(min_word_threshold=5)
    md_generator = DefaultMarkdownGenerator(content_filter=prune_filter)
    llm_config = LLMConfig(
        provider="gemini/gemini-2.0-flash",
        max_tokens=1000,
        top_p=1.0,
        frequency_penalty=0.0,
    )
    
    return CrawlerRunConfig(
        markdown_generator=md_generator,
        extraction_strategy=LLMExtractionStrategy(
            llm_config=llm_config,
            schema=NewsData.model_json_schema(),
            extraction_type="schema",
            extraction_instruction="Extract the news data from the markdown content",
            verbose=True,
        ),
    )


async def crawl_and_extract_content(
    url: str,
    crawler: AsyncWebCrawler,
    config: CrawlerRunConfig,
    base_data_dir: Path,
    target_domain: str,
) -> str | None:
    """
    Crawls a single URL, saves its markdown content, and returns the markdown.
    Returns None if crawling fails or content is not relevant.
    """
    logging.info(f"Processing URL: {url}")

    current_domain = get_domain(url)
    if not current_domain or current_domain.lower() != target_domain.lower():
        logging.warning(
            f"Skipping URL {url} as its domain '{current_domain}' does not match target '{target_domain}'"
        )
        return None

    url_fingerprint = get_url_fingerprint(url)
    # Path for storing data for this specific URL: data/<domain>/<fingerprint>/result.md
    data_storage_path_for_url = base_data_dir / current_domain / url_fingerprint
    data_storage_path_for_url.mkdir(parents=True, exist_ok=True)
    json_file_path = data_storage_path_for_url / "result.json"

    if json_file_path.exists():
        logging.info(
            f"JSON file already exists for {url} at {json_file_path}, reading from disk."
        )
        try:
            return json_file_path.read_text(encoding="utf-8")
        except Exception as e:
            logging.error(f"Error reading existing JSON file for {url}: {e}")
            # Proceed to re-crawl if reading fails

    try:
        result = await crawler.arun(url=url, config=config)
        if result.success and result.markdown:
            logging.info(f"Successfully crawled: {url}")
            with open(json_file_path, "w", encoding="utf-8") as f:
                f.write(result.extracted_content)
            logging.info(f"Stored JSON for {url} at {json_file_path}")
            return result.markdown
        else:
            logging.error(
                f"Failed to crawl or get JSON for {url}. Error: {result.error if result else 'Unknown error'}"
            )
            return None
    except Exception as e:
        logging.error(f"Exception during crawling {url}: {e}")
        return None 