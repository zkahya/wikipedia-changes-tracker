"""
Simple Wikipedia Scraper
"""
import requests
import json
import os
from datetime import datetime

def scrape_page(page_title, limit=20):
    """Scrape a Wikipedia page"""
    print(f" Scraping {page_title}...")
    
    url = "https://en.wikipedia.org/w/api.php"
    params = {
        "action": "query",
        "titles": page_title,
        "prop": "revisions",
        "rvprop": "timestamp|user|size|comment",
        "rvlimit": limit,
        "format": "json"
    }
    
    headers = {'User-Agent': 'WikipediaTracker/1.0'}
    
    try:
        response = requests.get(url, params=params, headers=headers)
        data = response.json()
        
        # Save raw data
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"data/{page_title}_{timestamp}.json"
        os.makedirs('data', exist_ok=True)
        
        with open(filename, 'w') as f:
            json.dump(data, f, indent=2)
        
        # Count revisions
        pages = data['query']['pages']
        page_id = list(pages.keys())[0]
        revisions = pages[page_id].get('revisions', [])
        
        print(f" Scraped {len(revisions)} revisions → {filename}")
        return filename
        
    except Exception as e:
        print(f" Error: {e}")
        return None

if __name__ == "__main__":
    # Test with multiple pages
    pages = ["Volkswagen_Golf", "Tesla_Model_3", "Electric_vehicle"]
    for page in pages:
        scrape_page(page, limit=10)
        print("---")
