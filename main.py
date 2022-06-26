import sqlalchemy
import pandas as pd
from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import datetime
import datetime
import sqlite3

DATABASE_LOCATION= "sqllite:////Users/julianaschuler/Library/CloudStorage/OneDrive-Persönlich/Privat/Python/spotify_pipeline.db"
USER_ID= "thejulles"
TOKEN= "BQACKUeD2hY4CgjAYBoviBAPdpIWJgWQ4WQTJ1sRXZsEz8MxEU6hScQ68zqrshJXhsv-NkefgDg4xlpbYO-deWjCvn4K22mI6GaUj7MjwoAwV2E5pftFHGO8LUfrAYwtrD1pCb6ROq_1Gmg-coJkPBWu0h_uCMwvPYQKS8wcToNSepg"

if __name__ == "__main__":

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": "Bearer {token}".format(token=TOKEN)
    }

    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000


    r = requests.get("https://api.spotify.com/v1/me/player/recently-played/?after={time}".format(time=yesterday_unix_timestamp), headers=headers)

    data = r.json()
    print(data)

    song_names = []
    artist_names = []
    played_at_list = []
    timestamps = []

    for song in data['items']:
        song_names.append(song['track']['name'])
        artist_names.append(song['track']['album']['artists'[0]]['name'])
        played_at_list.append(song['played_at'])
        timestamps.append(song['played_at'][0:10])

    song_dict = {
        "song_name" : song_names,
        "artist_name" : artist_names,
        "played_at_list" : played_at_list,
        "timestamps" : timestamps
    }

    song_df = pd.DataFrame(song_dict, columns=["song_name", "artist_name", "played_at", "timestamp"])

    print(song_df)