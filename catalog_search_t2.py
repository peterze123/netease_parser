"""Find all songs for given artist_id from find_artists_t1"""

import pandas as pd
import psycopg2
import requests, json

from misc import create_table, DB_PARAMS, API_HOST, NETEASE_PROFILE


def query_artist_ids():
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(**DB_PARAMS)
    cursor = conn.cursor()
    
    # SQL query to select all artist IDs
    select_query = "SELECT artist_id, search_term FROM artist;"
    
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
                'copyright_id': song.get('cp'),
                'no': song.get('no'),
                "album_id": song["al"]["id"],
                'json_string': song
            }
            
            # Add this song's info to our list
            extracted_data.append(song_info)
            
    return extracted_data
    

def catalog_insertion_query(catalog_li, netease_profile=None, search_term=None):
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(**DB_PARAMS)
    cursor = conn.cursor()
    
    audit_songs_args = []
    audit_json_args = []
    audit_finished_args = []
    
    # Prepare data for insertion (assuming the search_term and artist_search_user_profile are known)
    search_term = search_term

    # Iterate over the artists and insert each one
    for catalog in catalog_li:
        song_id = catalog['song_id']
        song_name = catalog['song_name']
        # tns = catalog['tns']
        artist_name = catalog['artist_name']
        artist_id = catalog['artist_id']
        fee = catalog['fee']
        pop = catalog['pop']
        mst = catalog['mst']
        cp = catalog['copyright_id']
        no = catalog['no']
        json_string = json.dumps(catalog['json_string'])
        
        audit_songs_args.append((song_id, song_name, artist_name, artist_id, fee, pop, mst, cp, no))
        audit_json_args.append((-1, song_id, json_string))
        audit_finished_args.append((artist_id,))
        
    audit_songs_args_str = ','.join(cursor.mogrify("(%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())", x).decode('utf-8') for x in audit_songs_args)
    audit_json_args_str = ','.join(cursor.mogrify("(%s, %s, %s)", x).decode('utf-8') for x in audit_json_args)
    audit_finished_str = ','.join(cursor.mogrify("(%s)", x).decode('utf-8') for x in audit_finished_args)
        
    try:
        cursor.execute(f"""
            INSERT INTO audit_songs (song_id, song_name, artist_name, artist_id, fee, popularity,
                mst, copyright_id, no, scrape_time 
            ) VALUES {audit_songs_args_str} ON CONFLICT (song_id) DO NOTHING;""")
        conn.commit()
        
        cursor.execute(f"""
            INSERT INTO audit_json (artist_id, song_id, api_text)
            VALUES {audit_json_args_str} ON CONFLICT DO NOTHING;""")
        conn.commit()
        
        cursor.execute(f"""
            update audit_artists_to_scrape as t set
            finished = True
            from (values
                {audit_finished_str}
            ) as c(artist_id) 
            where c.artist_id = t.artist_id;
        """)
        conn.commit()

    except psycopg2.DatabaseError as error:
        print("error: ", error)
        conn.rollback()

    # Close the cursor and the connection
    cursor.close()
    conn.close()

    
def create_t2_tables():#
    create_table(DB_PARAMS,
        """
            CREATE TABLE IF NOT EXISTS audit_songs (
                search_term TEXT,
                song_id BIGINT PRIMARY KEY,
                song_name TEXT,
                tns TEXT,
                artist_name TEXT,
                artist_id BIGINT,
                                fee INTEGER,
                popularity INTEGER,
                mst INTEGER,
                copyright_id BIGINT,
                no INTEGER,
                scrape_time TIMESTAMP
            );
        """
        )
        
    #The id that is not used should be -1
    create_table(DB_PARAMS,
        """
            CREATE TABLE IF NOT EXISTS audit_json (
                artist_id bigint NOT NULL,
                song_id bigint NOT NULL,
                api_text jsonb,
                CONSTRAINT audit_json_pkey PRIMARY KEY (artist_id, song_id)
            );
        """
        )


def get_all_artist_songs(artist_ids, skip_duplicates=False, search_term=None, create_dataframe=True):
    # create_t2_tables()
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

        if create_dataframe:
            catalog_df = pd.DataFrame.from_dict(cleaned_catalog_list)
            catalog_df = catalog_df.drop(columns=["json_string"])
            print(catalog_df)
            return catalog_df
            
        # injection into postgres
        catalog_insertion_query(cleaned_catalog_list, search_term=search_term)
        cleaned_catalog_list = []
        print("artist_id:", artist_id)
    
    print("task 2 complete")
    

if __name__ == '__main__':
    # search_term, artist_ids = query_artist_ids()
    search_term, artist_ids = "Marshmello", [233338]
    get_all_artist_songs(artist_ids)
    