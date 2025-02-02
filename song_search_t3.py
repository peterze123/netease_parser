"""Find all songs with the same title"""

import psycopg2
import requests, json

from misc import clean_song_json, create_table, DB_PARAMS, API_HOST, NETEASE_PROFILE, query

def get_raw_song_data(parent_path, keyword, search_type):
    path = '/'.join([parent_path, "search?keywords=" + keyword + "&type=" + str(search_type)])
    
    result = requests.get(path)
    result.raise_for_status()  # Will raise an HTTPError if the HTTP request returned an unsuccessful status code
    
    return json.dumps(result.json())

def song_insertion_query(cleaned_song_list, search_term, netease_profile):
    
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(**DB_PARAMS)
    cursor = conn.cursor()

    # This is the SQL query template for inserting data
    insert_query = """
    INSERT INTO song (
        artist_search_user_profile,
        search_term,
        song_id,
        song_name,
        song_trans,
        artist_name,
        artist_id,
        album_name,
        album_id,
        publish_time,
        copyright_id,
        status,
        fee,
        mark,
        size,
        mvid,
        json_string
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (song_id) DO NOTHING;
    """

    # Prepare data for insertion (assuming the search_term and artist_search_user_profile are known)
    artist_search_user_profile = netease_profile

    # Iterate over the artists and insert each one
    for i in cleaned_song_list:
        # Execute the insert query with the data
        cursor.execute(insert_query, (
            artist_search_user_profile,
            search_term,
            i["song_id"],
            i["song_name"],
            i["song_trans"],
            i["artist_name"],
            i["artist_id"],
            i["album_name"],
            i["album_id"],
            i["publish_time"],
            i["copyright_id"],
            i["status"],
            i["fee"],
            i["mark"],
            i["size"],
            i["mvid"],
            json.dumps(i['json_string'], ensure_ascii=False),
        ))

    # Commit the transaction
    conn.commit()

    # Close the cursor and the connection
    cursor.close()
    conn.close()

if __name__ == '__main__':
    create_table(DB_PARAMS,
        """
            CREATE TABLE IF NOT EXISTS song (
                artist_search_user_profile TEXT,
                search_term TEXT,
                song_id BIGINT PRIMARY KEY,
                song_name TEXT,
                song_trans TEXT,
                artist_name TEXT,
                artist_id TEXT,
                album_name TEXT,
                album_id BIGINT,
                publish_time TEXT,
                copyright_id BIGINT,
                status INTEGER,
                fee BIGINT,
                mark BIGINT,
                size INTEGER,
                mvid BIGINT,
                json_string TEXT
            );
        """
    )
    
    catalog_queries = [('song_name', 1), ('artist_name',100) , ('tns', 100)]
    
    # query from database
    for c in catalog_queries: 
        try:
            queried_list = query(DB_PARAMS, "SELECT " + c[0] + " FROM catalog;")
            queried_list = list(set(queried_list))
            
            for li in queried_list:
                raw_json = get_raw_song_data(API_HOST, li[0], c[1])
                cleaned_song_list = clean_song_json(raw_json)
                song_insertion_query(cleaned_song_list, c[0], NETEASE_PROFILE)
            
        except requests.exceptions.HTTPError as http_err:
            raise Exception(f"HTTP error occurred: {http_err}")  # HTTP error
        except requests.exceptions.RequestException as err:
            raise Exception(f"Error fetching profile: {err}")  # Other request issues
        
    print("task 3 complete")
        
