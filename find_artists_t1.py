""" 
1. Find the artist's NetEase profiles 
2. Find any duplicate or infringement profiles

Writes all artists with given or similar name to db"""

# -*- coding: utf-8 -*-
import psycopg2
import requests, json
import pandas as pd

from misc import create_table, get_id_from_netease_url, DB_PARAMS, API_HOST, NETEASE_PROFILE

def get_artist_json_from_name(name) -> dict:
    """Get all artists for given name. Also includes similar rtists"""
    path = '/'.join([API_HOST, 'search?keywords=' + name + '&type=100']) #type100 = search for artist
    
    result = requests.get(path)
    result.raise_for_status()  # Will raise an HTTPError if the HTTP request returned an unsuccessful status code
    
    return result.json()


def get_artist_json_from_link(profile_link):
    """find clear artist id from the actual link string"""
    artist_id = get_id_from_netease_url(profile_link)
    # find the correct path
    path = '/'.join([API_HOST, 'artists?id=' + artist_id])
    # access the artist-name search route
    try:
        response = requests.get(path)
        response.raise_for_status()  # Will raise an HTTPError if the HTTP request returned an unsuccessful status code
        
        # find the exact artist name
        artist_name = response.json()["artist"]["name"]
        
        return artist_name, get_artist_json_from_name(artist_name)
        
    except requests.exceptions.HTTPError as http_err:
        raise Exception(f"HTTP error occurred: {http_err}")  # HTTP error
    except requests.exceptions.RequestException as err:
        raise Exception(f"Error fetching profile: {err}")  # Other request issues


def artist_json_clean(data) -> dict:
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
    # cleaned_json = json.dumps(cleaned_data, indent=2,  ensure_ascii=False)
    
    return cleaned_data


def append_trans_artists(data_dict, api_server):
    """If any artists has a translation set request the artist page for this artist and add it to the artists field"""
    trans_artists = []
    for artist in data_dict['artists']:
        if artist["trans"] is not None:
            try:
                # request trans artist
                raw_json = get_artist_json_from_name(artist["trans"])
            except requests.exceptions.HTTPError as http_err:
                raise Exception(f"HTTP error occurred: {http_err}")  # HTTP error
            except requests.exceptions.RequestException as err:
                raise Exception(f"Error fetching profile: {err}")  # Other request issues
            
            cleaned_dict = artist_json_clean(raw_json)
            trans_artists += cleaned_dict['artists']
    
    data_dict['artists'] += trans_artists
    # return json.dumps(output_data, indent=2,  ensure_ascii=False)
    return data_dict


def artists_insertion_query(data, artist_name, netease_profile, raw_json):
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(**DB_PARAMS)
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

    # Prepare data for insertion (assuming the artist_name and artist_search_user_profile are known)
    artist_name = artist_name
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
            artist_name,
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


def create_t1_table():
        create_table(DB_PARAMS,
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


def get_all_artists_for_name(profile) -> pd.DataFrame:
    # create_t1_table()
    artist_name, raw_json = get_artist_json_from_link(profile)
    
    # cleaned version
    cleaned_dict = artist_json_clean(raw_json)
    cleaned_dict = append_trans_artists(cleaned_dict, API_HOST)
    
    artists_df = pd.DataFrame().from_dict(cleaned_dict["artists"])
    # print(artists_df)
    
    # injection into postgres
    artists_insertion_query(cleaned_dict, artist_name, profile, raw_json)
    print("task 1 complete")

    return artists_df 


if __name__ == '__main__':
    get_all_artists_for_name(NETEASE_PROFILE)
    
    