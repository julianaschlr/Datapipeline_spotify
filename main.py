import sqlalchemy
import pandas as pd
from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import datetime
import datetime
import sqlite3

DATABASE_LOCATION= "sqlite:///my_played_tracks.sqlite"
USER_ID= "thejulles"
TOKEN= "BQAQOdpX5mHK2ACcDuZwpZaGxKF0IDbBgOimqcmjV70n11go_kRbb7iJYJCdjPeslY0OXyggB6tJLKO1xZ6pW8hzKQG4EDUPNPzpheLZI_ijKhaChC2oJpYMtJn9MkMbfl4Mx0Y4MCchKAUn_Wkdimc55FKOsBexrcQ8devzhIln2OI"

# Generate your TOKEN here: https://developer.spotify.com/console/get-recently-played/?limit=&after=&before=
# Note: You need a spotify account


def check_if_valid_data(df: pd.DataFrame)-> bool:
    # check if the dataframe is empty
    if df.empty:
        print("No songs are downloaded. Finished execution.")
        return False

    # check if the primary is unique, so there aren't duplicates
    if pd.Series(df["played_at"]).is_unique:
        pass
    else:
        raise Exception("Primary Key Check is violated")

    # Check for Nulls
    if df.isnull().values.any():
        raise Exception("Null valued found")

    # Check if all timestamps are for yesterday's date
    yesterday = datetime.datetime.now() - datetime.timedelta(day=1)
    yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)

    timestamps = df["timestamp"].tolist()
    for timestamp in timestamps:
        if datetime.strptime(timestamp, "%Y-%m-%d") != yesterday:
           raise Exception("At least one of the returned songs does not come from within the last 24 hours.")

    return True


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

    for song in data["items"]:
        song_names.append(song["track"]["name"])
        artist_names.append(song["track"]["album"]["artists"][0]["name"])
        played_at_list.append(song["played_at"])
        timestamps.append(song["played_at"][0:10])

    song_dict = {
        "song_name": song_names,
        "artist_name": artist_names,
        "played_at": played_at_list,
        "timestamp": timestamps
    }

    song_df = pd.DataFrame(song_dict, columns=["song_name", "artist_name", "played_at", "timestamp"])

    # Validate
    if check_if_valid_data(song_df):
        print("Data valid, proceed to load stage.")

    # Load

    engine = sqlalchemy.create_engine(DATABASE_LOCATION)
    conn = sqlite3.connect("spotify_pipeline.sqlite")
    c = conn.cursor()


    sql_query = """
    CREATE TABLE IF NOT EXISTS my_played_tracks(
        song_name VARCHAR(200),
        artist_name VARCHAR(200),
        played_at VARCHAR(200),
        timestamp VARCHAR(200),
        CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
        )
    """

    c.execute(sql_query)
    try:
        song_df.to_sql("my_played_tracks", engine, index=False, if_exists ="fail")
    except ValueError:
        print("Data already exists in the Database")

    c.close()
    print("Closed successfully")