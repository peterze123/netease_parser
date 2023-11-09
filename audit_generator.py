"""Saves an audit as xlsx file for a given artist name on S3"""

import pandas as pd
import boto3
from sqlalchemy import create_engine
import time
import math
import datetime

from tqdm import tqdm
from misc import get_album_details, get_song_details, get_comments, get_follower_count
from find_artists_t1 import get_all_artists_for_name
from catalog_search_t2 import get_all_artist_songs

import warnings
from pandas.errors import SettingWithCopyWarning


ARTIST_NAME = "Mac Miller"
ARTIST_ID = 39884

API_HOST = 'http://18.119.235.232:3000'
NETEASE_PROFILE = 'https://music.163.com/#/artist?id=185871'

#https://music.163.com/#/artist?id=185871
ARTIST_NAME = ARTIST_NAME.lower()
#ignore the profile link generation error
warnings.simplefilter(action="ignore", category=SettingWithCopyWarning)


class AuditGenerator():
    def __init__(self, artist_name):
        self.artist_name = artist_name
        self.artist_id = ARTIST_ID
        
        self.audit_df = None
        self.duplicates_df = None
        self.db_engines = {}
        self.ssm_client = boto3.client('ssm')
        
        self.all_artists_df = get_all_artists_for_name(NETEASE_PROFILE)
        self.seperate_similar_fake_artists(self.all_artists_df)
        
        # print(self.duplicates_df)
    

    def get_database_engine(self, db_name):
        if db_name not in self.db_engines:
            db_params_dict = self.ssm_client.get_parameters(Names=["db_host", "db_pw"])
            db_host = db_params_dict["Parameters"][0]["Value"]
            db_pw = db_params_dict["Parameters"][1]["Value"]
            engine = create_engine(f"""postgresql+psycopg2://postgres:{db_pw}@{db_host}:5432/{db_name}""")
            self.db_engines[db_name] = engine
            
        return self.db_engines[db_name]
    
    
    def get_all_artist_songs(self) -> None:
        songs = get_all_artist_songs(ARTIST_NAME, [self.artist_id])[:5]
        # songs = get_all_artist_songs(ARTIST_NAME, [self.artist_id])
        
        self.audit_df = pd.DataFrame.from_dict(songs)
        self.audit_df = self.audit_df.rename(columns={"cp":"copyright_id", "pop": "popularity"})
        self.audit_df = self.audit_df.drop(columns = ["tns", "mst", "no"])
        self.audit_df = self.audit_df.reset_index()
        
        self.audit_df["song_url"] = self.audit_df.apply(lambda x: "https://music.163.com/#/song?id=" + str(x["song_id"]), axis=1)
        self.audit_df["scrape_time"] = datetime.datetime.now().strftime('%Y-%m-%d')
        
    
    def seperate_similar_fake_artists(self, all_artists_df):
        # self.duplicates_df = all_artists_df[all_artists_df["artist_id"] != self.artist_id]
        self.duplicates_df = all_artists_df.copy()
        
        self.duplicates_df["Platform"] = "Netease"
        self.duplicates_df["Type"] = ""
        self.duplicates_df["Notes"] = ""
        self.duplicates_df["Profile Link"] = "https://music.163.com/#/artist?id=" + self.duplicates_df["artist_id"].astype(str)
        
        self.duplicates_df = self.duplicates_df.rename(columns={"artist_name": "Profile Name"})
        self.duplicates_df = self.duplicates_df.drop(columns=["albumsize", "mvsize", "trans"])
         
    
    def convert_time_to_date(self, song_unixtime):
        try:
            if song_unixtime >= 0:
                release_date = datetime.datetime.utcfromtimestamp(song_unixtime).strftime('%Y-%m-%d')
            elif song_unixtime < 0:
                release_date = (datetime.datetime(1970, 1, 1) + datetime.timedelta(seconds=(song_unixtime))).strftime('%Y-%m-%d')
        except Exception as e:
            print(e)
            print(song_unixtime)
            release_date = "1900-1-1"
            
        return release_date
                
                
    def replace_major_labels(self):
        """Replace the chinese label names with western label names for major labels"""
        #load csv file
        major_labels_df = pd.read_csv("majors.csv")
        for i, chinese_label in enumerate(major_labels_df["c_label"]):
            self.audit_df["company"] = self.audit_df["company"].str.replace(chinese_label,
                                                                             chinese_label + " - " + major_labels_df.iloc[i]["w_label"])
        
            
    def add_folower_count(self) -> None:
        for artist_id in self.duplicates_df["artist_id"].to_list():
            follower_count = get_follower_count(artist_id)
            self.duplicates_df.loc[self.all_artists_df.artist_id == artist_id, 'follower_count'] = follower_count
            
        self.duplicates_df = self.duplicates_df.drop(columns=["artist_id"])
            
    
    def add_album_details(self) -> None:
        album_set = set(self.audit_df["album_id"].to_list()) - {0} 
        
        albums_dict = {}
        for album_id in tqdm(album_set):
            if album_id not in albums_dict:
                albums_dict[album_id] = {}
                
            album_dict = get_album_details(album_id)
            albums_dict[album_id]["album_name"] = album_dict["album"]["name"]
            albums_dict[album_id]["company"] = album_dict["album"]["company"]
            albums_dict[album_id]["realease_date"] = self.convert_time_to_date(album_dict["album"]["publishTime"]/1000)

        for album_id in album_set:
            self.audit_df.loc[self.audit_df.album_id == album_id, 'album_name'] = albums_dict[album_id]["album_name"]
            self.audit_df.loc[self.audit_df.album_id == album_id, 'company'] = albums_dict[album_id]["company"]
            self.audit_df.loc[self.audit_df.album_id == album_id, 'realease_date'] = albums_dict[album_id]["realease_date"]
            
    
    def add_artists_and_comments_to_songs(self) -> None:
        #TODO: test speed improvement with threading
        song_set = set(self.audit_df["song_id"].astype(str).to_list())
        songs_dict = {}
        
        for song in tqdm(get_song_details(song_set)["songs"]):
            song_id = song["id"]
            artists = [(artist["id"], artist["name"])  for artist in song["ar"]]
            comment_count = get_comments(song_id)["total"]
            songs_dict[song_id] = {"comment_count": comment_count, "artists": artists} 
        
        self.audit_df["comment_count"] = 0
        self.audit_df["artists"] = None
        
        for song_id in songs_dict:
            self.audit_df.loc[self.audit_df.song_id == song_id, 'comment_count'] = songs_dict[song_id]["comment_count"]
            
            artists_str = ", ".join([x[1] for x in songs_dict[song_id]["artists"]])
            self.audit_df.loc[self.audit_df.song_id == song_id, 'artists'] = artists_str
            
        # print(self.audit_df["comment_count"])
        # print(self.audit_df["artists"])
            
            
    def upload_audit_to_s3(self):
        pass
        
    
    def estimated_royalties(self, comment_count):
        if comment_count == "N/A" or comment_count < 1000:
            return "N/A"

        if comment_count >= 35000:
            min_royalty_estimate = comment_count
            
            #20k is base for calculations
            level = (math.floor(comment_count / 5000)) * 5000
            _10k_steps = ((level - 20000)  / 5000)
            max_royalty_estimate = (_10k_steps * 10000) + 65000 
        elif comment_count >= 30000:
            return "$30,000 - $85,000"
        elif comment_count >= 25000:
            return "$25,000 - $75,0000"
        elif comment_count >= 20000:
            return "$20,000 - $65,000+"
        elif comment_count >= 15000:
            return "$15,000 - $50,0000+"
        elif comment_count >= 10000:
            return "$10,000 to $30,000+"
        elif comment_count >= 5000:
            return "$5000 - $10000+"
        elif comment_count >= 2500:
            return "$2500 - $5000+" 
        elif comment_count >= 1000:
            return "$1000+"
            """Implement same pattern, add 5000 comments, then 1 to 1 value to get min amount, then add $10,000 to higher range"""
            
        return f"${min_royalty_estimate} - ${max_royalty_estimate}"
        
        
    def save_audit_as_xlsx(self):
        writer = pd.ExcelWriter("artist_audit.xlsx", engine = 'xlsxwriter')
        self.audit_df.to_excel(writer, index=True, sheet_name = "NetEase Raw")
        self.duplicates_df.to_excel(writer, index=True, sheet_name = "Profile Search")
        writer.close()
        
        
    def coloring_rows(self):
        copyright_ids_df = pd.read_csv("copyright_ids_netease.csv")
        red_copyrights = copyright_ids_df["Red"].to_list()
        # yellow_copyrights = copyright_ids_df["Yellow"].to_list()
        major_copyrights = copyright_ids_df["Majors"].to_list()
        red_labels = ["独立发行", "null"]
        
        #red copyright_ids
        self.audit_df["copyright_color"] = self.audit_df.apply(lambda x: 1 if x["copyright_id"] in red_copyrights 
                                                               else 0, axis=1)
        
        #red labels
        self.audit_df["copyright_color"] = self.audit_df.apply(lambda x: 1 if x["company"] in red_labels else x["copyright_color"], axis=1)
        
        #label majors
        self.audit_df["copyright_color"] = self.audit_df.apply(lambda x: 3 if x["copyright_color"] == 0 and x["copyright_id"] in major_copyrights else x["copyright_color"],
                                                                axis=1)
        
        #yellow color for comments > 3000
        self.audit_df["copyright_color"] = self.audit_df.apply(lambda x: 2 if x["copyright_color"] == 0 and
                                                                int(str(x["comment_count"]).replace("N/A", "0")) >= 3000 else x["copyright_color"], axis=1)
        
        def highlight_rows(df):
            column = ["copyright_color"]
            is_colored_red = pd.Series(data=False, index=df.index)
            is_colored_red[column] = df.loc[column] == 1
            
            is_colored_yellow = pd.Series(data=False, index=df.index)
            is_colored_yellow[column] = df.loc[column] == 2
            return ['background-color: red' if is_colored_red.any() else
                    ('background-color: yellow' if is_colored_yellow.any() else '') for v in is_colored_red]

        self.audit_df = self.audit_df.style.apply(highlight_rows, axis=1)
        
        # print(red_copyrights)
        # print(type(red_copyrights[0]))
    
        
    def generate_audit(self):
        self.get_all_artist_songs()
        self.add_album_details()
        self.add_artists_and_comments_to_songs()
        self.replace_major_labels()
        self.add_folower_count()
        self.audit_df["royalties"] = self.audit_df.apply(lambda x: self.estimated_royalties(x["comment_count"]), axis=1)
        self.coloring_rows()
        self.save_audit_as_xlsx()
        # self.audit_df = self.audit_df.drop(columns=["copyright_color"])
        self.upload_audit_to_s3()
        
        print(self.audit_df)
    
    
if __name__ == "__main__":
    start_time = time.time()
    audit_generator = AuditGenerator(ARTIST_NAME)
    # audit_generator.get_all_artist_songs()
    audit_generator.generate_audit()
    
    print("time:", time.time() - start_time)
    