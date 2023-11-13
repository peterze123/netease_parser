"""Get audit data for songs with score > 5 and save to db"""

from catalog_search_t2 import get_all_artist_songs
import psycopg2
from misc import DB_PARAMS

class AuditSpider():
    def __init__(self):
        pass
    
    
    def add_artist_data(self):
        pass
    #
    
    def add_song_data(self):
        """run task 2"""
        conn = psycopg2.connect(**DB_PARAMS)
        cursor = conn.cursor()
        
        cursor.execute("""SELECT * from audit_artists_to_scrape WHERE finished=FALSE""")
        artist_ids = [x[0] for x in cursor.fetchall()]
        
        get_all_artist_songs(artist_ids, create_dataframe=False)
        
        cursor.close()
        conn.close()
        
        
if __name__ == "__main__":
    audit_spider = AuditSpider()
    audit_spider.add_song_data()
    
