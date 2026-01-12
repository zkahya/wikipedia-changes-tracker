import requests
import sqlite3
import json
from datetime import datetime
import os

# 1. SCRAPE
print("1.  Scraping Wikipedia...")
url = "https://en.wikipedia.org/w/api.php"
params = {
    "action": "query",
    "titles": "Volkswagen_Golf",
    "prop": "revisions",
    "rvprop": "timestamp|user|size",
    "rvlimit": 10,
    "format": "json"
}
response = requests.get(url, params=params, headers={
    'User-Agent': 'DataPipelineDemo/1.0'
})
data = response.json()

# 2. SAVE
print("2.  Saving data...")
os.makedirs('data', exist_ok=True)
with open('data/sample.json', 'w') as f:
    json.dump(data, f, indent=2)

# 3. LOAD TO DATABASE
print("3.  Loading to database...")
conn = sqlite3.connect('database.db')
cursor = conn.cursor()
cursor.execute('''
    CREATE TABLE IF NOT EXISTS edits (
        id INTEGER PRIMARY KEY,
        timestamp TEXT,
        user TEXT,
        size INTEGER
    )
''')

pages = data['query']['pages']
for page_id, page_data in pages.items():
    for rev in page_data.get('revisions', []):
        cursor.execute(
            "INSERT INTO edits (timestamp, user, size) VALUES (?, ?, ?)",
            (rev['timestamp'], rev.get('user', 'Unknown'), rev['size'])
        )

conn.commit()

# 4. QUERY
print("4.  Analyzing...")
cursor.execute("SELECT COUNT(*) as total_edits FROM edits")
total = cursor.fetchone()[0]
cursor.execute("SELECT COUNT(DISTINCT user) as unique_users FROM edits")
users = cursor.fetchone()[0]

print(f"\n PIPELINE COMPLETE!")
print(f"   Total edits: {total}")
print(f"   Unique editors: {users}")
print(f"   Data saved: data/sample.json")
print(f"   Database: database.db")

conn.close()
