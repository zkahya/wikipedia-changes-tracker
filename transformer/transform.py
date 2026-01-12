"""
Simple Data Transformer
"""
import json
import sqlite3
import glob

def load_to_database():
    """Load all JSON files to database"""
    print(" Loading data to database...")
    
    conn = sqlite3.connect('database/wiki_edits.db')
    cursor = conn.cursor()
    
    # Create table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS edits (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            page TEXT,
            timestamp TEXT,
            user TEXT,
            size INTEGER,
            comment TEXT,
            date TEXT,
            hour INTEGER
        )
    ''')
    
    # Process all JSON files
    json_files = glob.glob('data/*.json')
    total_edits = 0
    
    for json_file in json_files:
        with open(json_file, 'r') as f:
            data = json.load(f)
        
        page_name = json_file.split('/')[-1].split('_')[0]
        pages = data['query']['pages']
        
        for page_id, page_data in pages.items():
            for rev in page_data.get('revisions', []):
                # Extract date and hour
                timestamp = rev['timestamp']
                date = timestamp.split('T')[0]
                hour = timestamp.split('T')[1].split(':')[0]
                
                cursor.execute('''
                    INSERT INTO edits (page, timestamp, user, size, comment, date, hour)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (
                    page_name,
                    timestamp,
                    rev.get('user', 'Anonymous'),
                    rev.get('size', 0),
                    rev.get('comment', ''),
                    date,
                    hour
                ))
                total_edits += 1
    
    conn.commit()
    
    # Get statistics
    cursor.execute("SELECT COUNT(DISTINCT page) FROM edits")
    page_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(DISTINCT user) FROM edits")
    user_count = cursor.fetchone()[0]
    
    print(f" Loaded {total_edits} edits from {len(json_files)} files")
    print(f" Statistics:")
    print(f"   - Pages tracked: {page_count}")
    print(f"   - Unique editors: {user_count}")
    print(f"   - Total edits: {total_edits}")
    
    # Show sample
    cursor.execute("SELECT page, date, COUNT(*) as edits FROM edits GROUP BY page, date LIMIT 5")
    print(f"\n Sample time-series data:")
    for row in cursor.fetchall():
        print(f"   {row[0]} on {row[1]}: {row[2]} edits")
    
    conn.close()

if __name__ == "__main__":
    load_to_database()
