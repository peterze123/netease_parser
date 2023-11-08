# -*- coding: utf-8 -*-
import psycopg2
import requests, json

from misc import create_table, get_id_from_netease_url, db_params, api_host, netease_profile

def get_artist_json_from_name(name, parent_path):
    # access the actual route
    path = '/'.join([parent_path, 'search?keywords=' + name + '&type=100'])
    
    result= requests.get(path)
    result.raise_for_status()  # Will raise an HTTPError if the HTTP request returned an unsuccessful status code
    
    return result.json()

def get_artist_json_from_link(profile_link, parent_path):
    # find clear artist id from the actual link string
    artist_id = get_id_from_netease_url(profile_link)
    # find the correc path
    path = '/'.join([parent_path, 'artists?id=' + artist_id])
    # access the artist-name search route
    try:
        response = requests.get(path)
        response.raise_for_status()  # Will raise an HTTPError if the HTTP request returned an unsuccessful status code
        
        # find the exact artist name
        artist_name = response.json()["artist"]["name"]
        
        return artist_name, get_artist_json_from_name(artist_name, parent_path)
        
    except requests.exceptions.HTTPError as http_err:
        raise Exception(f"HTTP error occurred: {http_err}")  # HTTP error
    except requests.exceptions.RequestException as err:
        raise Exception(f"Error fetching profile: {err}")  # Other request issues

def artist_json_clean(data):
    # Prepare a container for cleaned data
    cleaned_data = {
        "artistcount": data['result']['artistCount'],
        "hlWords": data['result']['hlWords'],
        "artists": []
    }

    # Extract the required information for each artist
    for artist in data['result']['artists']:
        artist_info = {
            "artist_id": artist['id'],
            "artist_name": artist['name'],
            "albumsize": artist['albumSize'],
            "mvsize": artist['mvSize'],
            "trans": artist.get('trans') if artist.get('trans') else artist.get('transNames')[0] if artist.get('transNames') else None
        }
        cleaned_data["artists"].append(artist_info)
        
    # Convert cleaned data back to JSON string if necessary
    cleaned_json = json.dumps(cleaned_data, indent=2,  ensure_ascii=False)
    
    return cleaned_json

def append_trans_artists(raw_data, parent_path):
    output_data = json.loads(raw_data)
    data = json.loads(raw_data)
    
    for artist in data['artists']:
        if artist["trans"] is not None:
            try:
                raw_json = get_artist_json_from_name(artist["trans"], parent_path)
            except requests.exceptions.HTTPError as http_err:
                raise Exception(f"HTTP error occurred: {http_err}")  # HTTP error
            except requests.exceptions.RequestException as err:
                raise Exception(f"Error fetching profile: {err}")  # Other request issues
            
            cleaned_json = artist_json_clean(raw_json)
            output_data['artists'] += json.loads(cleaned_json)['artists']
            
    return json.dumps(output_data, indent=2,  ensure_ascii=False)

def artists_insertion_query(json_data, search_term, netease_profile, raw_json):
    # Parse the JSON string into a Python dictionary
    data = json.loads(json_data)

    # Connect to the PostgreSQL database
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()

    # This is the SQL query template for inserting data
    insert_query = """
    INSERT INTO artist (
        artist_search_user_profile,
        search_term,
        artist_name,
        artist_id,
        trans,
        artistcount,
        hlwords,
        albumsize,
        mvsize,
        json_string
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (artist_id) DO NOTHING;
    """

    # Prepare data for insertion (assuming the search_term and artist_search_user_profile are known)
    search_term = search_term
    artist_search_user_profile = netease_profile
    artistcount = data['artistcount']
    hlwords = json.dumps(data['hlWords'], ensure_ascii=False)  # Convert the list of words to JSON string

    # Iterate over the artists and insert each one
    for artist in data['artists']:
        artist_name = artist['artist_name']
        artist_id = artist['artist_id']
        albumsize = artist['albumsize']
        mvsize = artist['mvsize']
        trans = artist['trans']

        # Execute the insert query with the data
        cursor.execute(insert_query, (
            artist_search_user_profile,
            search_term,
            artist_name,
            artist_id,
            trans,
            artistcount,
            hlwords,
            albumsize,
            mvsize,
            json.dumps(raw_json, ensure_ascii=False),
        ))

    # Commit the transaction
    conn.commit()

    # Close the cursor and the connection
    cursor.close()
    conn.close()


if __name__ == '__main__':
    create_table(db_params,
        """
            CREATE TABLE IF NOT EXISTS artist (
                artist_search_user_profile TEXT,
                search_term TEXT,
                artist_name TEXT,
                artist_id BIGINT PRIMARY KEY,
                trans TEXT,
                artistcount INTEGER,
                hlwords TEXT,
                albumsize INTEGER,
                mvsize INTEGER,
                json_string TEXT
            );
        """
    )
    
    search_term, raw_json = get_artist_json_from_link(netease_profile, api_host)
    
    # cleaned version
    cleaned_json = artist_json_clean(raw_json)
    cleaned_json = append_trans_artists(cleaned_json, api_host)

    print(cleaned_json)
    
    # injection into postgres
    artists_insertion_query(cleaned_json, search_term, netease_profile, raw_json)
    
    print("task 1 complete")