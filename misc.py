"""library for audit"""

from lxml.html import fromstring
import requests, json, psycopg2
from typing import Iterable

# Replace these variables with your database credentials
db_params = {
    'database': 'temp',
    'user': 'temp',
    'password': 'lsJKas2Sjk',
    'host': 'cma.cps7ukarrgmb.us-east-1.rds.amazonaws.com',
    'port': '5432'
}

API_HOST = 'http://18.119.235.232:3000'
NETEASE_PROFILE = 'https://music.163.com/#/artist?id=185871'


def create_table(db_params, query):
    """table creation functions"""
    # Establish a connection to the database
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()
    
    # SQL statement to create a table if it does not exist
    create_table_query = query
    
    # Execute the create table query
    cursor.execute(create_table_query)
    
    # Commit the changes
    conn.commit()
    
    # Close cursor and connection
    cursor.close()
    conn.close()
    # print("Table created successfully or already exists.")
    
    
def query(db_params, query):
    """Establish a connection to the database"""
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()

    # Execute the selection
    cursor.execute(query)
    # Fetch all the results
    result = cursor.fetchall()
    
    return result


def get_artist_name_from_xpath(profile_link):
    try:
        response = requests.get(profile_link)
        response.raise_for_status()  # Will raise an HTTPError if the HTTP request returned an unsuccessful status code
        
        # Parse the HTML content if the request was successful
        soup = fromstring(response.text)
        artist_elements = soup.xpath('//h2[@id="artist-name"]/text()')
        
        if not artist_elements:
            raise ValueError("No artist name found in the profile.")
        
        artist_name = artist_elements[0].strip()
        
        if not artist_name:
            raise ValueError("The artist name is empty.")
        
        return artist_name

    except requests.exceptions.HTTPError as http_err:
        raise Exception(f"HTTP error occurred: {http_err}")  # HTTP error
    except requests.exceptions.RequestException as err:
        raise Exception(f"Error fetching profile: {err}")  # Other request issues
    except ValueError as ve:
        raise Exception(f"Profile parsing issue: {ve}")  # Parsing issues


def get_song_details(song_ids: Iterable) -> dict:
    song_ids_string = ",".join(song_ids)
    url_song_api = f"{API_HOST}/song/detail?ids={song_ids_string}"
    response = requests.get(url_song_api)
    response.raise_for_status()
    
    return response.json()


def get_album_details(album_id) -> dict:
    url_album_api = f"{API_HOST}/album?id={album_id}"
    response = requests.get(url_album_api)
    response.raise_for_status()
    
    return response.json()


def get_follower_count(artist_id) -> int:
    url_album_api = f"{API_HOST}/artist/follow/count?id={artist_id}"
    response = requests.get(url_album_api)
    response.raise_for_status()
    
    return response.json()["data"]["fansCnt"]


def get_comments(song_id, limit=1):
    url_comment_api = f"{API_HOST}/comment/event?threadId=R_SO_4_{song_id}&limit={limit}"
    
    response = requests.get(url_comment_api)
    response.raise_for_status()

    return response.json()


def get_id_from_netease_url(url):
    """get id= from netease urls
        The fragment will be after the last '/', split the fragment if it exists
        Split the fragment by '&' in case there are multiple parameters"""
    params = url.split('?')[-1].split('&')
    
    for param in params:
        # Split each parameter by '=' to separate the key and value
        key_value = param.split('=')
        if key_value[0] == 'id':
            return key_value[1]
    return None


if __name__ == "__main__":
    get_comments(347230)
    pass
    
