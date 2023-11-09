"""library for audit"""

from lxml.html import fromstring
import requests, json, psycopg2

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

def clean_song_json(data):
    data = json.loads(data)

    songs_info = []

    if 'songs' in data['result'] and data['code'] == 200:
        # Extracting information from each song
        for song in data['result']['songs']:
            songs_info.append({
                'song_id': song['id'],
                'song_name': song['name'],
                'song_trans': song.get('alias', []),
                'artist_name': ','.join([str(artist['name']) for artist in song['artists']]),
                'artist_id': ','.join([str(artist['id']) for artist in song['artists']]),
                'album_id': song['album']['id'],
                'album_name': song['album']['name'],
                'publish_time': song['album']['publishTime'],
                'copyright_id': song['copyrightId'],
                'status': song['status'],
                'fee': song['fee'],
                'mark': song.get('mark', 0),
                'size': song['album'].get('size', 0),
                'mvid': song.get('mvid', 0),
                'json_string': data
            })
            
    return songs_info

if __name__ == "__main__":
    pass
    
