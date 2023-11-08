"""Find all songs for given artist_id from find_artists_t1"""

import psycopg2
import requests, json

from misc import create_table, db_params, api_host, netease_profile

def query_artist_ids():
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()
    
    # SQL query to select all artist IDs
    select_query = "SELECT artist_id, search_term  FROM Artist;"
    
    try:
        # Execute the SQL query
        cursor.execute(select_query)
        
        # Fetch all the results
        artist_ids = cursor.fetchall()
        
        return artist_ids[0][1], [i[0] for i in artist_ids]
            
    except psycopg2.Error as e:
        print(f"An error occurred: {e}")
    finally:
        # Close the cursor and the connection
        cursor.close()
        conn.close()

def get_song_size(artist_id, parent_path):
    try:
        # make a request to the songs route to find total songs
        path = '/'.join([parent_path, 'artist/songs?id=' + str(artist_id)])
        
        result= requests.get(path)
        result.raise_for_status()  # Will raise an HTTPError if the HTTP request returned an unsuccessful status code
        
        return path, int(result.json()["total"])
    
    except requests.exceptions.HTTPError as http_err:
        raise Exception(f"HTTP error occurred: {http_err}")  # HTTP error
    except requests.exceptions.RequestException as err:
        raise Exception(f"Error fetching profile: {err}")  # Other request issues

def get_catalog_json(offset, parent_path):
    try:
        # make a request to the songs route to find total songs
        path = '&'.join([parent_path, 'offset=' + str(offset) + '&limit=100'])
        
        result = requests.get(path)
        result.raise_for_status()  # Will raise an HTTPError if the HTTP request returned an unsuccessful status code
        
        return result.json()

    except requests.exceptions.HTTPError as http_err:
        raise Exception(f"HTTP error occurred: {http_err}")  # HTTP error
    except requests.exceptions.RequestException as err:
        raise Exception(f"Error fetching profile: {err}")  # Other request issues

    
def catalog_json_clean(data):
    
    extracted_data = []
    
    # Loop through each song in the JSON data
    for song in data['songs']:
        # Each song has a list of artists, loop through each one
        for artist in song.get('ar', []):
            # Extract the required fields
            song_info = {
                'song_id': song.get('id'),
                'song_name': song.get('name'),
                'tns': song.get('alia', []),
                'artist_name': artist.get('name'),
                'artist_id': artist.get('id'),
                'fee': song.get('fee'),
                'pop': song.get('pop'),
                'mst': song.get('mst'),
                'cp': song.get('cp'),
                'no': song.get('no'),
                'json_string': data
            }
            # Add this song's info to our list
            extracted_data.append(song_info)
            
    return extracted_data
    
def catalog_insertion_query(catalog_li, search_term, netease_profile):

    # Connect to the PostgreSQL database
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()

    # This is the SQL query template for inserting data
    insert_query = """
    INSERT INTO catalog (
        artist_search_user_profile,
        search_term,
        song_id ,
        song_name,
        tns,
        artist_name,
        artist_id,
        fee,
        pop,
        mst,
        cp,
        no,
        json_string
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (song_id) DO NOTHING;
    """

    # Prepare data for insertion (assuming the search_term and artist_search_user_profile are known)
    search_term = search_term
    artist_search_user_profile = netease_profile

    # Iterate over the artists and insert each one
    for i in catalog_li:
        song_id = i['song_id']
        song_name = i['song_name']
        tns = i['tns']
        artist_name = i['artist_name']
        artist_id = i['artist_id']
        fee = i['fee']
        pop = i['pop']
        mst = i['mst']
        cp = i['cp']
        no = i['no']
        json_string = i['json_string']

        # Execute the insert query with the data
        cursor.execute(insert_query, (
            artist_search_user_profile,
            search_term,
            song_id,
            song_name,
            tns,
            artist_name,
            artist_id,
            fee,
            pop,
            mst,
            cp,
            no,
            json.dumps(json_string, ensure_ascii=False),
        ))

    # Commit the transaction
    conn.commit()

    # Close the cursor and the connection
    cursor.close()
    conn.close()
    
if __name__ == '__main__':
    create_table(db_params,
        """
            CREATE TABLE IF NOT EXISTS catalog (
                artist_search_user_profile TEXT,
                search_term TEXT,
                song_id BIGINT PRIMARY KEY,
                song_name TEXT,
                tns TEXT,
                artist_name TEXT,
                artist_id BIGINT,
                fee INTEGER,
                pop INTEGER,
                mst INTEGER,
                cp BIGINT,
                no INTEGER,
                json_string TEXT
            );
        """
    )
    
    search_term, artist_ids = query_artist_ids()
    
    cleaned_catalog_list = []
    
    for i in artist_ids:
        path, size = get_song_size(i, api_host)
        # access the catalogs
        counter = 0
        
        while counter < size + 100:
            raw_data = get_catalog_json(counter, path)
            cleaned_catalog_list += catalog_json_clean(raw_data)
            
            counter += 100
            
    
    # injection into postgres
    catalog_insertion_query(cleaned_catalog_list, search_term, netease_profile)
    
    print("task 2 complete")