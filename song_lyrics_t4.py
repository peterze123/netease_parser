"""Get lyrics for list of song_ids and save them to db"""

import re
import psycopg2
import requests, json

from misc import create_table, DB_PARAMS, API_HOST, query

BATCH_SIZE = 50 #size of lyrics written in one query


def get_raw_lyric_data(songid) -> dict:
    path = f"{API_HOST}/lyric?id={str(songid)}"
    
    result = requests.get(path)
    result.raise_for_status()  # Will raise an HTTPError if the HTTP request returned an unsuccessful status code

    return result.json()


def clean_lyric_json(data):
    # Extracting fields
    transuser_id = data.get('transUser', {}).get('id', "none")
    #check if this song has lyrics
    pure_music = data['puremusic'] if 'puremusic' in data else False
    
    lyric_info = {"pure_music": pure_music}

    if transuser_id:
        lyric_info['user_status'] = data.get('transUser', {}).get('status', -1)
        lyric_info['user_id'] = data.get('transUser', {}).get('userid', -1)
        lyric_info['uptime'] = data.get('transUser', {}).get('uptime', 'N/A')
        lyric_info['version'] = data.get('lrc', {}).get('version', -1)
        lyric_info['lyrics'] = data.get('lrc', {}).get('lyric', 'N/A')
        lyric_info['tlyrics'] = data.get('tlyric', {}).get('lyric', 'Not found') if data.get('tlyric') else None
        lyric_info['json_string'] = data

        # Extract songwriters/artists
        # since we are passing this to the router header, it would search all possibilities both in the songwriters column
        lyric_info['songwriters'] = re.findall(r'作词 : (.*?)\\n', lyric_info['lyrics'])
        lyric_info['songwriters'] += re.findall(r'作曲 : (.*?)\\n', lyric_info['lyrics'])
        
        return lyric_info
    #a song without any lyrics
    else:
        lyric_info['user_status'] = 0
        lyric_info['songwriters'] = []
        lyric_info['user_id'] = 0
        lyric_info['uptime'] = 'N/A'
        lyric_info['version'] = data.get('lrc', {}).get('version', -1)
        lyric_info['lyrics'] = data.get('lrc', {}).get('lyric', 'N/A')
        lyric_info['tlyrics'] = data.get('tlyric', {}).get('lyric', 'N/A') if data.get('tlyric') else None
        lyric_info['json_string'] = data
        
        return lyric_info
    
    return {}


def songlyric_insertion_query(lyric_dicts: list):
    """Writes all dicts of lyric_dicts in one query"""
    audit_lyrics_args = []
    audit_json_args = []
    audit_finished_args = []
    
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(**DB_PARAMS)
    cursor = conn.cursor()
    
    for lyric_dict in lyric_dicts:
        if lyric_dict == {}:
            continue
        
        song_id = lyric_dict["song_id"]
        is_music_only = lyric_dict["pure_music"]
        songwriters = lyric_dict["songwriters"]
        user_status = lyric_dict["user_status"]
        user_id = lyric_dict["user_id"]
        uptime = lyric_dict["uptime"]
        version = lyric_dict["version"]
        lyrics = lyric_dict["lyrics"]
        tlyrics = lyric_dict["tlyrics"]
        api_text = json.dumps(lyric_dict["json_string"], ensure_ascii=False)
        
        audit_lyrics_args.append((song_id, is_music_only, songwriters, user_status, user_id, uptime, version, lyrics, tlyrics))
        audit_json_args.append((song_id, api_text))
        audit_finished_args.append((song_id,))
    
    
    audit_lyrics_args_str = ','.join(cursor.mogrify("(%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())", x).decode('utf-8') for x in audit_lyrics_args)
    audit_json_args_str = ','.join(cursor.mogrify("(%s, %s)", x).decode('utf-8') for x in audit_json_args)
    audit_finished_str = ','.join(cursor.mogrify("(%s)", x).decode('utf-8') for x in audit_finished_args)

    try:
        cursor.execute(f"""
            INSERT INTO audit_song_lyrics (
                song_id, 
                is_music_only,
                songwriters,
                user_status,
                user_id,
                uptime,
                version,
                lyrics,
                tlyrics,
                scrape_time
            ) VALUES {audit_lyrics_args_str}"""
        )
        conn.commit()
    except psycopg2.DatabaseError as error:
        print("db error: ", error)
        
    try:
        cursor.execute(f"""
            Insert INTO audit_json (lyric_song_id, api_text)
            VALUES {audit_json_args_str}
            ON CONFLICT DO NOTHING"""
        )
        conn.commit()
    except psycopg2.DatabaseError as error:
        print("db error: ", error)
        
    try:
        cursor.execute(f"""
            update audit_songs_to_scrape as t set
            lyrics_finished = True
            from (values
                {audit_finished_str}
            ) as c(song_id) 
            where c.song_id = t.song_id;
        """)
        conn.commit()
    except psycopg2.DatabaseError as error:
        print("db error: ", error)

    # Close the cursor and the connection
    cursor.close()
    conn.close()


def create_t4_table():
    #TODO: lyric_id not really needed. Maybe better use song_id + version as pk
    create_table(DB_PARAMS,
        """
        CREATE TABLE IF NOT EXISTS audit_song_lyrics
            (
                song_id bigint,
                lyric_id bigint,
                is_music_only boolean DEFAULT false,
                songwriters text COLLATE pg_catalog."default",
                user_status integer,
                user_id bigint,
                uptime text COLLATE pg_catalog."default",
                version integer,
                scrape_time timestamp without time zone,
                lyrics text COLLATE pg_catalog."default",
                tlyrics text COLLATE pg_catalog."default",
                CONSTRAINT audit_song_lyrics_pkey PRIMARY KEY (lyric_id)
            );
        
        CREATE SEQUENCE IF NOT EXISTS lyric_id_seq OWNED BY audit_song_lyrics.lyric_id ;
        ALTER  TABLE audit_song_lyrics ALTER COLUMN lyric_id SET DEFAULT nextval('lyric_id_seq'::regclass);
        """
    )
   
    
def get_lyrics_for_songs(song_ids):
    batch_counter = 0
    lyric_dicts = []
    for song_id in song_ids:
        try:
            song_lyrics_raw = get_raw_lyric_data(song_id)
            song_lyrics_dict = clean_lyric_json(song_lyrics_raw)
            song_lyrics_dict["song_id"] = song_id
            lyric_dicts.append(song_lyrics_dict)
            batch_counter += 1
            
            if batch_counter % BATCH_SIZE == 0:
                print("batch:", (batch_counter / 50))
                songlyric_insertion_query(lyric_dicts)
                lyric_dicts.clear()
                
        except requests.exceptions.HTTPError as http_err:
            raise Exception(f"HTTP error occurred: {http_err}")  # HTTP error
        except requests.exceptions.RequestException as err:
            raise Exception(f"Error fetching profile: {err}")  # Other request issues
               
    if len(lyric_dicts) > 0:
        songlyric_insertion_query(lyric_dicts)
        

if __name__ == '__main__':
    # create_t4_table()
    # quit()
    
    # queried_list = query(DB_PARAMS, "SELECT song_id, artist_name FROM song;")
    # song_ids = [28285776] #with trans user
    
    songs_without_lyrics = [x[0] for x in query(DB_PARAMS, "SELECT song_id FROM audit_songs_to_scrape WHERE lyrics_finished = FALSE")]
    
    # song_ids = [17437594, 1851698391, 28285776] #without trans user
    # get_lyrics_for_songs(song_ids)
    get_lyrics_for_songs(songs_without_lyrics)

    # print("task 4 complete")
        
    