"""Search the API for any songs with the same lyrics all the songs in the artists catalog."""

import re
import psycopg2
import requests, json

from misc import create_table, db_params, API_HOST,NETEASE_PROFILE, query, clean_song_json

def clean_lyrics(raw_lyrics):
    # Regular expression to match timestamps and lines containing '作词' or '作曲'
    pattern = re.compile(r'\[.*?\]|\n.*(作词|作曲).*')

    # Replace the matched patterns with a newline character
    lyrics_only = re.sub(pattern, '\n', raw_lyrics)

    # Split the resulting string into a list by newlines and remove empty lines
    lyrics_list = [line.strip() for line in lyrics_only.split('\n') if line.strip()]
    
    return list(set(lyrics_list[1:]))

def get_raw_song_data(parent_path, search_term):
    path = '/'.join([parent_path, "search?keywords=" + search_term + "&type=1006"])
    
    result = requests.get(path)
    result.raise_for_status()  # Will raise an HTTPError if the HTTP request returned an unsuccessful status code
    
    return json.dumps(result.json())

def lyric_insertion_query(cleaned_song_list, search_term, netease_profile):
    
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()

    # This is the SQL query template for inserting data
    insert_query = """
    INSERT INTO lyric (
        artist_search_user_profile,
        search_term,
        song_id,
        song_name,
        artist_name,
        artist_id,
        publish_time,
        copyright_id,
        status,
        fee,
        mark,
        size,
        json_string
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
            i["publish_time"],
            i["copyright_id"],
            i["status"],
            i["fee"],
            i["mark"],
            i["size"],
            json.dumps(i['json_string'], ensure_ascii=False),
        ))

    # Commit the transaction
    conn.commit()

    # Close the cursor and the connection
    cursor.close()
    conn.close()

if __name__ == '__main__':
    create_table(db_params,
        """
            CREATE TABLE IF NOT EXISTS lyric (
                artist_search_user_profile TEXT,
                search_term TEXT,
                song_id BIGINT PRIMARY KEY,
                song_name TEXT,
                artist_name TEXT,
                artist_id TEXT,
                publish_time TEXT,
                copyright_id BIGINT,
                status INTEGER,
                fee BIGINT,
                mark BIGINT,
                size INTEGER,
                json_string TEXT
            );
        """
    )
    
    queried_list = query(db_params, "SELECT lyrics, songwriters FROM songlyric;")

    for lyrics, songwriters in queried_list:
        try:
            cleaned_lyrics_list = clean_lyrics(lyrics)
            # traverse through each line of the lyrics to search for result
            for line in cleaned_lyrics_list:
                raw_song_data = get_raw_song_data(API_HOST, line)
                cleaned_song_data = clean_song_json(raw_song_data)
                lyric_insertion_query(cleaned_song_data, line, NETEASE_PROFILE)
            # after lyric search do song writers serach as well
            raw_song_data = get_raw_song_data(API_HOST, songwriters)
            cleaned_song_data = clean_song_json(raw_song_data)
            lyric_insertion_query(cleaned_song_data, songwriters, NETEASE_PROFILE)
        
        except requests.exceptions.HTTPError as http_err:
            raise Exception(f"HTTP error occurred: {http_err}")  # HTTP error
        except requests.exceptions.RequestException as err:
            raise Exception(f"Error fetching profile: {err}")  # Other request issues
    
    print("task 4 complete")