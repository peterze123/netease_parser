"""Search the API for all keywords from other tables: song_name, artist_name """

import re
import psycopg2
import requests, json

from misc import create_table, DB_PARAMS, API_HOST,NETEASE_PROFILE, query, clean_song_json

def get_raw_song_data(parent_path, search_term):
    path = '/'.join([parent_path, "search?keywords=" + search_term])
    
    result = requests.get(path)
    result.raise_for_status()  # Will raise an HTTPError if the HTTP request returned an unsuccessful status code
    
    return json.dumps(result.json())

def general_insertion_query(cleaned_song_list, search_term, netease_profile):
    
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(**DB_PARAMS)
    cursor = conn.cursor()

    # This is the SQL query template for inserting data
    insert_query = """
    INSERT INTO general (
        artist_search_user_profile,
        search_term,
        song_id,
        song_name,
        artist_name,
        artist_id,
        album_name,
        album_id,
        publish_time,
        copyright_id,
        status,
        duration,
        alias,
        fee,
        mark,
        size,
        mvid,
        json_string
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (song_id) DO NOTHING;
    """

    # Prepare data for insertion (assuming the search_term and artist_search_user_profile are known)
    search_term = search_term
    artist_search_user_profile = netease_profile

    # Iterate over the artists and insert each one
    for i in cleaned_song_list:
        # Execute the insert query with the data
        cursor.execute(insert_query, (
            artist_search_user_profile,
            search_term,
            i["song_id"],
            i["song_name"],
            i["artist_name"],
            i["artist_id"],
            i["album_name"],
            i["album_id"],
            i["publish_time"],
            i["copyright_id"],
            i["status"],
            i["duration"],
            i["alias"],
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
           CREATE TABLE IF NOT EXISTS general (
                artist_search_user_profile TEXT,
                search_term TEXT,
                song_id BIGINT PRIMARY KEY,
                song_name TEXT,
                artist_name TEXT,
                artist_id TEXT,
                album_name TEXT,
                album_id BIGINT,
                publish_time TEXT,
                copyright_id BIGINT,
                status INTEGER,
                duration INTEGER,
                alias TEXT,
                fee BIGINT,
                mark BIGINT,
                size INTEGER,
                mvid BIGINT,
                json_string TEXT
            );
        """
    )
    
    queried_song_list = query(DB_PARAMS, """ SELECT song_name FROM song;""")
    queried_artist_list = query(DB_PARAMS, """SELECT artist_name FROM artist;""")
    
    try:
        for song_name in queried_song_list:
            raw_song_data = get_raw_song_data(API_HOST, song_name[0])
            cleaned_song_data = clean_song_json(raw_song_data)
            general_insertion_query(cleaned_song_data, song_name[0], NETEASE_PROFILE)
    
        for artist_name in queried_artist_list:
            raw_song_data = get_raw_song_data(API_HOST, artist_name[0])
            cleaned_song_data = clean_song_json(raw_song_data)
            general_insertion_query(cleaned_song_data, artist_name[0], NETEASE_PROFILE)
            
        
    except requests.exceptions.HTTPError as http_err:
            raise Exception(f"HTTP error occurred: {http_err}")  # HTTP error
    except requests.exceptions.RequestException as err:
            raise Exception(f"Error fetching profile: {err}")  # Other request issues
        
    print("task 6 complete")
    
    
    