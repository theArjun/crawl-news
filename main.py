"""
Main application for crawl-news: BFS web crawler for news articles.
"""

import asyncio
import logging
from collections import deque
from crawl4ai import AsyncWebCrawler

from utils.config import INITIAL_URL, DATA_STORE_DIR, setup_config
from utils.url_utils import get_domain, extract_news_urls
from utils.crawler_utils import create_crawler_config, crawl_and_extract_content


async def main(start_url: str):
    """
    Main function to perform BFS crawling starting from start_url.
    """
    base_domain = get_domain(start_url)
    if not base_domain:
        logging.error(
            f"Could not determine base domain from initial URL: {start_url}. Exiting."
        )
        return

    logging.info(
        f"Starting crawl with initial URL: {start_url} (Base Domain: {base_domain})"
    )

    crawler_config = create_crawler_config()
    urls_to_visit = deque([start_url])
    visited_urls = set()

    async with AsyncWebCrawler() as crawler:
        while urls_to_visit:
            current_url = urls_to_visit.popleft()

            if current_url in visited_urls:
                logging.info(f"Skipping already visited URL: {current_url}")
                continue

            visited_urls.add(current_url)

            markdown_content = await crawl_and_extract_content(
                url=current_url,
                crawler=crawler,
                config=crawler_config,
                base_data_dir=DATA_STORE_DIR,
                target_domain=base_domain,
            )

            if markdown_content:
                new_urls = extract_news_urls(markdown_content, base_domain, current_url)
                for new_url in new_urls:
                    if new_url not in visited_urls and new_url not in urls_to_visit:
                        urls_to_visit.append(new_url)
                        logging.info(f"Added to queue: {new_url}")

            # Optional: Add a small delay to be polite to the server
            await asyncio.sleep(1)  # 1-second delay

    logging.info(f"Crawling finished. Visited {len(visited_urls)} URLs.")


if __name__ == "__main__":
    setup_config()
    asyncio.run(main(INITIAL_URL))
