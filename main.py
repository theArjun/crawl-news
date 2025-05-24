import asyncio
from crawl4ai import (
    AsyncWebCrawler,
    PruningContentFilter,
    DefaultMarkdownGenerator,
    CrawlerRunConfig,
)
from urllib.parse import urlparse, urljoin, parse_qs
from pathlib import Path
import re
from collections import deque
import logging
import hashlib

# --- Configuration ---
# Initial URL to start crawling from
INITIAL_URL = "https://merolagani.com/NewsDetail.aspx?newsID=114689"
# Directory to store the crawled data
DATA_STORE_DIR = Path(__file__).parent / "data"
# Create the base data store directory if it doesn't exist
DATA_STORE_DIR.mkdir(parents=True, exist_ok=True)

# Setup logging
# Change level to logging.DEBUG for more verbose output
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# --- Helper Functions ---


def get_domain(url: str) -> str:
    """Extracts the domain (netloc) from a URL."""
    try:
        parsed_url = urlparse(url)
        return parsed_url.netloc
    except Exception as e:
        logging.error(f"Error parsing domain from URL '{url}': {e}")
        return ""


def get_url_fingerprint(url: str) -> str:
    """
    Generates an MD5 hash of the URL's path and query string for unique directory naming.
    """
    parsed_url = urlparse(url)
    data_to_hash = parsed_url.path
    if parsed_url.query:
        data_to_hash += "?" + parsed_url.query
    return hashlib.md5(data_to_hash.encode("utf-8")).hexdigest()


def extract_news_urls(
    markdown_content: str, base_domain: str, current_page_url: str
) -> set[str]:
    """
    Extracts news URLs (matching the NewsDetail.aspx pattern on the base_domain)
    from the given markdown content.
    """
    extracted_urls = set()

    # Regex for Markdown links: [text](url)
    md_links_pattern = r"\[[^\]]*\]\(([^)]+)\)"
    potential_urls_from_md_links = re.findall(md_links_pattern, markdown_content)

    # Regex for plain URLs (simplified, focuses on finding things that look like URLs)
    # This pattern aims to find absolute URLs or paths starting with '/'
    plain_urls_pattern = r'(?:https?://[^\s"\'()<>]+|/[^\s"\'()<>]+)'
    potential_plain_urls = re.findall(plain_urls_pattern, markdown_content)

    all_potential_strings = potential_urls_from_md_links + potential_plain_urls

    logging.debug(
        f"Found {len(all_potential_strings)} potential URL strings in markdown from {current_page_url}"
    )

    for url_candidate_str in all_potential_strings:
        url_candidate_str = url_candidate_str.strip().strip("'\"")  # Clean up quotes

        # Quick check for the core pattern before more expensive parsing
        if not (
            "newsdetail.aspx" in url_candidate_str.lower()
            and "newsid=" in url_candidate_str.lower()
        ):
            continue

        absolute_url = url_candidate_str
        # Resolve relative URLs
        if url_candidate_str.startswith("/"):
            absolute_url = urljoin(current_page_url, url_candidate_str)
        elif not url_candidate_str.startswith(("http://", "https://")):
            # Handle cases like "NewsDetail.aspx?newsID=123" (relative to current path segment)
            # or "subfolder/NewsDetail.aspx?newsID=123"
            absolute_url = urljoin(current_page_url, url_candidate_str)

        # Normalize URL: ensure scheme, remove fragment
        parsed_temp_url = urlparse(absolute_url)
        current_page_scheme = urlparse(current_page_url).scheme

        # Ensure scheme if missing (e.g. if url_candidate_str was "merolagani.com/News...")
        if not parsed_temp_url.scheme:
            if parsed_temp_url.netloc:  # If it looks like a domain is present
                parsed_temp_url = parsed_temp_url._replace(scheme=current_page_scheme)
            else:  # If it's just a path, urljoin should have handled it, but double check
                absolute_url = urljoin(
                    current_page_url, absolute_url
                )  # Re-join if needed
                parsed_temp_url = urlparse(absolute_url)

        absolute_url = parsed_temp_url._replace(fragment="").geturl()

        # Final validation
        parsed_new_url = urlparse(absolute_url)
        if (
            parsed_new_url.netloc
            and parsed_new_url.netloc.lower() == base_domain.lower()
            and "/newsdetail.aspx" in parsed_new_url.path.lower()
            and "newsid=" in parsed_new_url.query.lower()
        ):
            # Check if newsID is numeric
            query_params = parse_qs(parsed_new_url.query)
            news_id_values = query_params.get(
                "newsID", []
            )  # parse_qs returns list of values
            if news_id_values and news_id_values[0].isdigit():
                extracted_urls.add(absolute_url)
                logging.debug(
                    f"EXTRACTED valid news URL: {absolute_url} (from: {url_candidate_str})"
                )
            else:
                logging.debug(
                    f"REJECTED candidate (non-numeric newsID): {absolute_url} (from: {url_candidate_str})"
                )
        else:
            logging.debug(
                f"REJECTED candidate (domain/path/query mismatch): {absolute_url} (from: {url_candidate_str})"
            )

    return extracted_urls


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
    markdown_file_path = data_storage_path_for_url / "result.md"

    if markdown_file_path.exists():
        logging.info(
            f"Markdown already exists for {url} at {markdown_file_path}, reading from disk."
        )
        try:
            return markdown_file_path.read_text(encoding="utf-8")
        except Exception as e:
            logging.error(f"Error reading existing markdown for {url}: {e}")
            # Proceed to re-crawl if reading fails

    try:
        result = await crawler.arun(url=url, config=config)
        if result.success and result.markdown:
            logging.info(f"Successfully crawled: {url}")
            with open(markdown_file_path, "w", encoding="utf-8") as f:
                f.write(result.markdown)
            logging.info(f"Stored markdown for {url} at {markdown_file_path}")
            return result.markdown
        else:
            logging.error(
                f"Failed to crawl or get markdown for {url}. Error: {result.error if result else 'Unknown error'}"
            )
            return None
    except Exception as e:
        logging.error(f"Exception during crawling {url}: {e}")
        return None


# --- Main Application Logic ---
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

    # Step 1: Create a pruning filter (same as original)
    prune_filter = PruningContentFilter(min_word_threshold=5)
    # Step 2: Insert it into a Markdown Generator
    md_generator = DefaultMarkdownGenerator(content_filter=prune_filter)
    # Step 3: Pass it to CrawlerRunConfig
    crawler_config = CrawlerRunConfig(markdown_generator=md_generator)

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
    asyncio.run(main(INITIAL_URL))
