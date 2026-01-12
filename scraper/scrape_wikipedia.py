"""
Wikipedia Changes Scraper
Extracts page revision history using Wikipedia's free API
"""

import requests
import json
from datetime import datetime
import os
import time

def scrape_page_revisions(page_title, limit=50):
    """
    Scrape Wikipedia revision history
    """
    print(f"Scraping {page_title}...")
    
    url = "https://en.wikipedia.org/w/api.php"
    
    headers = {
        'User-Agent': 'WikipediaChangesTracker/1.0 (https://github.com/yourusername/wikipedia-changes-tracker)'
    }
    
    params = {
        "action": "query",
        "titles": page_title,
        "prop": "revisions",
        "rvprop": "timestamp|user|size|comment|ids",
        "rvlimit": limit,
        "format": "json"
    }
    
    try:
        time.sleep(1)
        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status()
        data = response.json()
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"data/raw/{page_title}_{timestamp}.json"
        os.makedirs("data/raw", exist_ok=True)
        
        with open(filename, 'w') as f:
            json.dump(data, f, indent=2)
        
        pages = data.get('query', {}).get('pages', {})
        page_id = list(pages.keys())[0]
        revisions = pages[page_id].get('revisions', [])
        
        print(f"Success: Scraped {len(revisions)} revisions -> {filename}")
        return filename
        
    except Exception as e:
        print(f"Error scraping {page_title}: {e}")
        return None

if __name__ == "__main__":
    scrape_page_revisions("Volkswagen_Golf", limit=10)
