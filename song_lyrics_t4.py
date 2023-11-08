"""Find each songs lyrics"""

import re
import psycopg2
import requests, json

from misc import create_table, db_params, api_host, netease_profile, query

def get_raw_lyric_data(parent_path, songid):
    path = '/'.join([parent_path, "lyric?id=" + str(songid)])
    
    result = requests.get(path)
    result.raise_for_status()  # Will raise an HTTPError if the HTTP request returned an unsuccessful status code
    
    return json.dumps(result.json())

def clean_lyric_json(data, artistname):
    data = json.loads(data)
    
    # Extracting fields
    lyric_id = data.get('transUser', {}).get('id', 'Not found')
    
    lyric_info = {
        'lyric_id' : lyric_id
    }
    
    if lyric_id != 'Not found':
        lyric_info['lyric_status'] = data.get('transUser', {}).get('status', 'Not found')
        lyric_info['user_id'] = data.get('transUser', {}).get('userid', 'Not found')
        lyric_info['uptime'] = data.get('transUser', {}).get('uptime', 'Not found')
        lyric_info['version'] = data.get('lrc', {}).get('version', 'Not found')
        lyric_info['lyrics'] = data.get('lrc', {}).get('lyric', 'Not found')
        lyric_info['json_string'] = data

        # Extract songwriters/artists
        # since we are passing this to the router header, it would search all possibilities both in the songwriters column
        lyric_info['songwriters'] = re.findall(r'作词 : (.*?)\\n', lyric_info['lyrics'])
        lyric_info['songwriters'] += re.findall(r'作曲 : (.*?)\\n', lyric_info['lyrics'])
        
        if lyric_info['songwriters'] == []:
            lyric_info['songwriters'] = artistname
        
        return lyric_info
    
    return {}

def lyric_insertion_query(cleaned_song_list, search_term, netease_profile):
    
    if cleaned_song_list == {}:
        return
    
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()
    
    insert_query = """
    INSERT INTO songlyric (
        artist_search_user_profile,
        search_term,
        lyric_id,
        lyric_status,
        user_id,
        uptime,
        version,
        lyrics,
        songwriters,
        json_string
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (lyric_id) DO NOTHING;
    """
    
    search_term = search_term
    artist_search_user_profile = netease_profile
    
    cursor.execute(insert_query, (
            artist_search_user_profile,
            search_term,
            cleaned_song_list["lyric_id"],
            cleaned_song_list["lyric_status"],
            cleaned_song_list["user_id"],
            cleaned_song_list["uptime"],
            cleaned_song_list["version"],
            cleaned_song_list["lyrics"],
            cleaned_song_list["songwriters"],
            json.dumps(cleaned_song_list['json_string'], ensure_ascii=False),
        ))
    
    # Commit the transaction
    conn.commit()

    # Close the cursor and the connection
    cursor.close()
    conn.close()

        
if __name__ == '__main__':
    create_table(db_params,
        """
            CREATE TABLE IF NOT EXISTS songlyric (
                artist_search_user_profile TEXT,
                search_term TEXT,
                lyric_id BIGINT PRIMARY KEY,
                lyric_status INTEGER,
                user_id BIGINT,
                uptime TEXT,
                version INTEGER,
                lyrics TEXT,
                songwriters TEXT,
                json_string TEXT
            );
        """
    )
    
    queried_list = query(db_params, "SELECT song_id, artist_name FROM song;")
    
    for i in queried_list:
        raw_json = get_raw_lyric_data(api_host, i[0])
        cleaned_json = clean_lyric_json(raw_json, i[1])
        print(cleaned_json)
        lyric_insertion_query(cleaned_json, i[0], api_host)
        
    print("task 4 complete")
        