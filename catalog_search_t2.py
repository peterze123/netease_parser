"""Find all songs for given artist_id from find_artists_t1"""

import pandas as pd
import psycopg2
import requests, json

from misc import create_table, db_params, API_HOST, NETEASE_PROFILE


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


def get_song_size(artist_id, api_url):
    try:
        # make a request to the songs route to find total songs
        path = '/'.join([api_url, 'artist/songs?id=' + str(artist_id)])
        
        result = requests.get(path)
        result.raise_for_status()  # Will raise an HTTPError if the HTTP request returned an unsuccessful status code
        
        return path, int(result.json()["total"])
    
    except requests.exceptions.HTTPError as http_err:
        raise Exception(f"HTTP error occurred: {http_err}")  # HTTP error
    except requests.exceptions.RequestException as err:
        raise Exception(f"Error fetching profile: {err}")  # Other request issues


def get_catalog_dict(offset, parent_path) -> dict:
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

    
def catalog_clean(data: dict) -> list[dict]:
    
    extracted_data = []
    
    # Loop through each song in the data dict
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
<<<<<<< HEAD
                "album_id": song["al"]["id"],
                'json_string': data
=======
                'json_string': song
>>>>>>> branch 'netease_api_scraper' of https://git-codecommit.us-east-1.amazonaws.com/v1/repos/cma_libs
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
    for catalog in catalog_li:
        song_id = catalog['song_id']
        song_name = catalog['song_name']
        tns = catalog['tns']
        artist_name = catalog['artist_name']
        artist_id = catalog['artist_id']
        fee = catalog['fee']
        pop = catalog['pop']
        mst = catalog['mst']
        cp = catalog['cp']
        no = catalog['no']
        json_string = catalog['json_string']

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

    
def create_t2_table():#
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


def get_all_artist_songs(search_term, artist_ids, skip_duplicates=False):
    create_t2_table()
    cleaned_catalog_list = []
    
    for artist_id in artist_ids:
        path, size = get_song_size(artist_id, API_HOST)
        # access the catalogs
        counter = 0
        
        #TODO: better use pagination
        while counter < size + 100:
            data_dict = get_catalog_dict(counter, path)
            cleaned_catalog_list += catalog_clean(data_dict)

            counter += 100

    catalog_df = pd.DataFrame.from_dict(cleaned_catalog_list)
    catalog_df = catalog_df.drop(columns=["json_string"])

    # injection into postgres
    # catalog_insertion_query(cleaned_catalog_list, search_term, NETEASE_PROFILE)
    
    print("task 2 complete")
    return catalog_df
    

if __name__ == '__main__':
    # search_term, artist_ids = query_artist_ids()
<<<<<<< HEAD
    search_term, artist_ids = "Porter Robinson", [185871]
    # print(get_song_size(artist_ids[0], API_HOST))
=======
    search_term, artist_ids = "Marshmello", [1060019]
>>>>>>> branch 'netease_api_scraper' of https://git-codecommit.us-east-1.amazonaws.com/v1/repos/cma_libs
    get_all_artist_songs(search_term, artist_ids)
    