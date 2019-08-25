import os
import glob
import psycopg2
import pandas as pd
import datetime
from sql_queries import *


def process_song_file(cur, filepath):
"""
    This function uses pandas's read_json method to parse the song json file.  It will then extract the data for song object and artist object, then insert those into database.
    
    Parameters: 
        cur (cursor): The cursor object created when making the database connection
        filepath: The absolute path to the json file for function to parse
        
    Returns:
        None
"""
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0]
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values[0]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    This function uses pandas read_json method to parse the activity log file.  It filters the record by keeping the "NextSong" action first, then construct the data for time, users, and songplay object before insert them into the database.
    
    Parameters: 
        cur (cursor):The cursor object created when making the database connection
        filepath: The absolute path to the json file for function to parse
        
    Returns:
        None
"""
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    df["timestamp"] = pd.to_datetime(df.ts, unit='ms')
    t = pd.to_datetime(df.timestamp)
    
    # insert time data records
    time_data = (t, t.dt.hour, t.dt.day, t.dt.weekofyear, t.dt.month, t.dt.year, t.dt.weekday)
    column_labels = ('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    time_df = pd.DataFrame(dict(zip(column_labels, time_data)))



    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[["userId", "firstName", "lastName", "gender", "level"]]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
    
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record

        if songid != None and artistid != None:
            cur.execute(songplay_table_insert, (row.timestamp, row.userId, row.level, songid, artistid, row.sessionId,row.location, row.userAgent ))


def process_data(cur, conn, filepath, func):
    """
    This function uses pandas's read_json method to parse the song json file.  It will then extract the data for song object and artist object, then insert those into database.
    
    Parameters: 
        cur (cursor): The cursor object created when making the database connection
        conn (connection):  The connection object created to make the database connection
        filepath: The absolute path to parent folder that holds the json files.
        func (function): This parameter allows the code to make function call on the passed in method.
        
    Returns:
        None
"""
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
    This is the main function of the etl.py script. It creates the database connection first, then calls the process_data method to load both song and activity logs to insert to database.
    Parameters: 
        None
        
    Returns:
        None
"""
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()