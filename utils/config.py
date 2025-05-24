"""
Configuration constants and setup for the crawl-news application.
"""

import logging
from pathlib import Path
from dotenv import load_dotenv

# Application constants
INITIAL_URL = "https://merolagani.com/NewsDetail.aspx?newsID=114689"
DATA_STORE_DIR = Path(__file__).parent.parent / "data"

# Initialize configuration
def setup_config():
    """Initialize application configuration."""
    DATA_STORE_DIR.mkdir(parents=True, exist_ok=True)
    load_dotenv()
    
    logging.basicConfig(
        level=logging.INFO, 
        format="%(asctime)s - %(levelname)s - %(message)s"
    ) 