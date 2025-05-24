"""
URL utility functions for the crawl-news application.
"""

import re
import hashlib
import logging
from urllib.parse import urlparse, urljoin, parse_qs


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