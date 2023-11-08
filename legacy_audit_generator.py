"""Saves an audit as xlsx file for a given artist name on S3"""
"""TODO: Run on Lambda or ECS?"""

#TODO: add qq support 

import pandas as pd
import boto3 
# import psycopg2
import asyncio
import requests
import math
# from thefuzz import fuzz
# from thefuzz import process
from sqlalchemy import create_engine
from netease_max.artist_scraper import main
from song_enhancer import scrape_song_url

# ARTIST_NAME = "Marshmello"
# ARTIST_ID = 1060019

ARTIST_NAME = "Porter Robinson"
ARTIST_ID = 185871

#https://music.163.com/#/artist?id=185871

ARTIST_NAME = ARTIST_NAME.lower()

HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/118.0",
        "Cookie": "NMTID=00OdWsZcsSV45GS6017tHaWpWpF5esAAAGKl8cODQ; JSESSIONID-WYYY=%2Fbta6w11FtT%5CCMChcvZ6VF%5CTisC3Zr13xT0mWTfuxkJ2DZ2sE9WHZFqo%2BA4iEEEtMIJJ8Y419IkNaIOeEqG8C0OV80WvjdUXVZAQJ9ofCWSX62qbpCScBIG5YsT5iokqY4V%5C8kVc1dbvDQIOYx0p0g06BP9eSwdkAgt6QmeOIpJ%5CYTZu%3A1698166523458; _iuqxldmzr_=32; _ntes_nnid=0b2e1003d10319d308896e9467fbb9ed,1694763518469; _ntes_nuid=0b2e1003d10319d308896e9467fbb9ed; WEVNSM=1.0.0; WNMCID=fmenyc.1694763520918.01.0; WM_NI=UuTD2woPJa256QJ2yQqTDXqM7N%2FaLm%2Bmj369837IeGNHgYTYFGShcrZtU%2F1Lso7FMGchGEuf1XvC682sEeZaUYX8ZIQIT1n4fQNMzw8KdyjKNCTmBYG%2Bktwq8b1Mq58vUlE%3D; WM_NIKE=9ca17ae2e6ffcda170e2e6ee86d46385bb9bb2d14ab49e8ab3d84a839b9facc173bba89cd0d43c8cecf899c82af0fea7c3b92a97ab9ca5b17dad908682b13ff68f86d3fc5bb38c8cb9f447f592e182cf67e9b3a6d4b533fcedf9aae84d8e98a9bad75386999d89e27a9aac9bd0b448b1b1f9a7d04da1b889a6f86ef388baa4b534f2ad8b86d96b978798d9d94393979cd2ed40a68f82aef97dfc95ac9ae9418e9d9c8ec15385f5a595f67bfcbeffaef43ffcaa9b8dc837e2a3; WM_TID=2TBsE8aitdhEUEQVFUaFaBaxLa3z9ASn; ntes_utid=tid._.TnMWOLLIzShBAwRVERPVgTAJhm11zy8m._.0; sDeviceId=YD-vSiL8hdWZdBABhFUUULUfD0ZYmoOFnJV; playerid=74619777",
}


class AuditGenerator():
    def __init__(self, artist_name):
        self.artist_name = artist_name
        self.db_engines = {}
        self.ssm_client = boto3.client('ssm')


    def get_database_connection(self, db_name):
        if db_name not in self.db_engines:
            db_params_dict = self.ssm_client.get_parameters(Names=["db_host", "db_pw"])
            db_host = db_params_dict["Parameters"][0]["Value"]
            db_pw = db_params_dict["Parameters"][1]["Value"]
            engine = create_engine(f"""postgresql+psycopg2://postgres:{db_pw}@{db_host}:5432/{db_name}""")
            self.db_engines[db_name] = engine
            
        return self.db_engines[db_name]
    
        
    def get_all_artist_songs(self):
        #run scraper that gets all songs for given artist_id
        loop = asyncio.get_event_loop()
        task = loop.create_task(main(ARTIST_ID))
        songs = loop.run_until_complete(task)
        
        self.audit_df = pd.DataFrame.from_dict(songs)
        self.audit_df = self.audit_df.drop(columns = ["song_json"])
        self.audit_df = self.audit_df.reset_index()
        
        print(self.audit_df["song_name"])
        print(self.audit_df.columns)
        print(self.audit_df["song_url"])
    
    
    def find_similar_fake_artists(self):
        #TODO: add follower count, etc.
        
        #netease data
        url = f"http://music.163.com/api/search/pc?type=100&s={ARTIST_NAME}&limit=50&offset=0"
        print(url)
        
        response = requests.get(url, headers=HEADERS)
        results = response.json()
        if response.status_code == 200 and "462" not in str(results["code"]):
            netease_duplicates_df = pd.DataFrame.from_dict(results["result"]["artists"])
        else:
            raise Exception("Replace cookie in HEADERS")
        
        columns_to_remove = list(netease_duplicates_df.columns.difference(['id','name']))
        netease_duplicates_df["Platform"] = "Netease Cloud Music"
        netease_duplicates_df = netease_duplicates_df.drop(columns=columns_to_remove)  
        netease_duplicates_df["Profile Link"] = "https://music.163.com/#/artist?id=" + netease_duplicates_df["id"].astype(str)
        netease_duplicates_df = netease_duplicates_df.drop(columns=['id'])
        
        netease_duplicates_df["Type"] = ""
        netease_duplicates_df["Notes"] = ""
        netease_duplicates_df = netease_duplicates_df.rename(columns={'name': 'Profile Name'})
        
        netease_duplicates_df = netease_duplicates_df.iloc[:, [1,0,3,4,2]]
                
        # print(netease_duplicates_df)
        # print(netease_duplicates_df.columns)
        
        #qq data
        
        # combined_duplicates_df = pd.concat(netease_duplicates_df)
        # duplicates_df["Type"] = ""
        # duplicates_df["Notes"] = ""
        
        self.duplicates_df = netease_duplicates_df
        

    def replace_major_labels(self):
        """Replace the chinese label names with western label names for major labels"""
        #load csv file
        major_labels_df = pd.read_csv("majors.csv")
        for i, chinese_label in enumerate(major_labels_df["c_label"]):
            self.audit_df["company"] = self.audit_df["company"].str.replace(chinese_label,
                                                                             chinese_label + " - " + major_labels_df.iloc[i]["w_label"])
            
    
    def enrich_audit_df(self):
        """Add comment_count, album_name, release_date to the dataframe"""
        #TODO: more speed!!!
        self.audit_df["comment_count"] = "N/A"
        self.audit_df["album_name"] = "N/A"
        self.audit_df["release_date"] = "N/A"
        
        for row_number in range(len(self.audit_df.index)):
            result_tuple = scrape_song_url(row_number, self.audit_df)
            
            self.audit_df.at[row_number, "comment_count"] = result_tuple[0]
            self.audit_df.at[row_number, "album_name"] = result_tuple[1]
            self.audit_df.at[row_number, "release_date"] = result_tuple[2]
            
        # print(self.audit_df)
    

    def copy_audit_to_s3(self):
        pass
    
    
    def add_duplicates_column(self):
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
        self.duplicates_df.to_excel(writer, index=True, sheet_name = "Duplicate Profile Accounts")
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
        self.find_similar_fake_artists()
        self.enrich_audit_df()
        self.replace_major_labels()
        self.add_duplicates_column()
        self.audit_df["royalties"] = self.audit_df.apply(lambda x: self.estimated_royalties(x["comment_count"]), axis=1)
        self.coloring_rows()
        self.save_audit_as_xlsx()
        self.copy_audit_to_s3()
        
        print(self.audit_df)
    
    
if __name__ == "__main__":
    audit_generator = AuditGenerator(ARTIST_NAME)
    audit_generator.generate_audit()
    