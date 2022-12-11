import os
import glob
import psycopg2
import json
import pandas as pd
from io import StringIO 
from sql_queries import *


def get_files(filepath):
    '''
    Gets a list of the nested file names in a directory.

    Parameters:
        filepath (str):The string of the directory path which contains the file names to be extracted.

    Returns:
        None.   
    '''

    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))
    
    return all_files




def merge_json_files(files):
    '''
    Combines nested json files in a directory into one new json file in the same directory as this script

    Parameters:
        files (list): A list of the file paths which contains the json files to be combined.

    Returns:
        fname: (str): A string of the new json file name
    '''
    result = list()
    fname = 'all_songs.json'
    for f in files:
        with open(f, 'r') as infile:
            result.append(json.load(infile))

    with open(fname, 'w') as output_file:
        json.dump(result, output_file, indent=0)
        return fname
        
        
        

def merge_Log_files(files):
    df_temp = None
    for f in files:
        if df_temp is not None:
            df_temp = df_temp.append(pd.read_json(f, lines=True), ignore_index=True)
        elif df_temp is None:
            df_temp = pd.read_json(f, lines=True)
    return df_temp




def process_songs_file(cur, conn, filepath):
    '''
    Processes the song data and inserts the appropriate data into the songs and artists database tables.

    Parameters:
        cur (psycopg2.cursor): A database cursor
        conn (psycopg2.connection): A database connection
        filepath (str): A string of the filepath to the songs file

    Returns:
        None.   
    '''
    # Getting file paths
    files = get_files(filepath)
    
    # Merging song record files 
    file = merge_json_files(files)
    
    # Open songs file
    df = pd.read_json("all_songs.json", lines=False)

    # Extracting song data
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']]
    
    # Dropping duplicate "song_id" values
    song_data.drop_duplicates(subset='song_id', keep='last', inplace=True)

    # Saving the song data to a csv file
    song_fname = 'songs.csv'
    song_data.to_csv(song_fname, index=False) # Removing the extra csv index between to match the columns for bulk data insert
    
    # Getting the current working directory path
    working_dir = os.getcwd()
    song_data_path = f"{working_dir}/{song_fname}"
    
    try:
        # Copying the data from the csv file to the database table
        with open(song_data_path, 'r') as f:
            cur.copy_expert(sql=song_table_bulk_insert, file=f)
            conn.commit()
            
    except psycopg2.Error as err:
        print(f"error inserting song data: {err}")


    # Extracting artist data
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']]
    
    # Dropping duplicate "song_id" values
    artist_data.drop_duplicates(subset='artist_id', keep='last', inplace=True)

    # Saving artist data to a csv file
    artist_fname = 'artists.csv'
    artist_data.to_csv(artist_fname, index=False) # Removing the extra csv index between to match the columns for bulk data insert

    # Getting the current working directory path
    working_dir = os.getcwd()
    artist_data_path = f"{working_dir}/{artist_fname}"
    
    try:
        # Copying the data from the csv to the database table
        with open(artist_data_path, 'r') as f:
            cur.copy_expert(sql=artist_table_bulk_insert, file=f)
            conn.commit()
            
    except psycopg2.Error as err:
        print(f"error inserting artist data: {err}")


        

def process_logs_file(cur, conn, filepath):
    '''
    Processes the log data and inserts the appropriate data into the users, time and songplays database tables.

    Parameters:
        cur (psycopg2.cursor): A database cursor
        conn (psycopg2.connection): A database connection
        filepath (str): A string of the filepath to the songs file

    Returns:
        None.   
    '''
    # Getting file paths
    files = get_files(filepath)
    
    # open logs file
    df = merge_Log_files(files)

    # filter by NextSong action
    df = df[df['page']=='NextSong']
    
    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'] , unit='ms')
    
    # Extracting time data records
    time_data = (t, t.dt.hour, t.dt.day, t.dt.week , t.dt.month, t.dt.year, t.dt.weekday)
    column_labels = ('timestamp', 'hour', 'day', 'week of year', 'month', 'year', 'weekday')
    time_df = pd.DataFrame.from_dict(dict(zip(column_labels,time_data)))

    # Saving time data into a csv file
    time_fname = 'time.csv'
    time_df.to_csv(time_fname, index=False) # Removing the extra csv index between to match the columns for bulk data insert

    # Getting the current working directory path
    working_dir = os.getcwd()
    time_data_path = f"{working_dir}/{time_fname}"
    
    try:
        # Copying the time data from the csv file into the database table
        with open(time_data_path, 'r') as f:
            cur.copy_expert(sql=time_table_bulk_insert, file=f)
            conn.commit()
    
    except psycopg2.Error as err:
        print(f"error inserting time data: {err}")


    # loading user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]
    
    # Transorming the "userId" column data type from object to int
    user_df.userId = pd.to_numeric(user_df.userId)

    # Dropping dublicate values
    user_df.drop_duplicates(subset='userId', inplace=True)

    # Saving the data into a csv file
    user_fname = 'users.csv'
    user_df.to_csv(user_fname, index=False) # Removing the extra csv index between to match the columns for bulk data insert

    # Getting the current working directory path
    working_dir = os.getcwd()
    user_data_path = f"{working_dir}/{user_fname}"
    
    try:
        # Copying the user data from the csv file into the database table
        with open(user_data_path, 'r') as f:
            cur.copy_expert(sql=user_table_bulk_insert, file=f)
            conn.commit()
    
    except psycopg2.Error as err:
        print(f"error inserting user data: {err}")
        
    
    # Extracting songplay data
    records_df = df[['ts', 'userId', 'level', 'sessionId','location' ,'userAgent', 'song']]
    records_df['ts'] = pd.to_datetime(records_df['ts'] , unit='ms')

    # Saving the data into a csv file
    records_fname = 'records.csv'
    records_df.to_csv(records_fname, index=False) # Removing the extra csv index between to match the columns for bulk data insert

    # Getting the current working directory path
    working_dir = os.getcwd()
    records_data_path = f"{working_dir}/{records_fname}"
    
    try:
        # Copying the songplay data from the csv file into the database table and maatching records by song name
        with open(records_data_path, 'r') as f:
            cur.copy_expert(sql=songplay_table_bulk_insert, file=f)
            conn.commit()
    
    except psycopg2.Error as err:
        print(f"error inserting song data: {err}")


        
        
def process_data(cur, conn, filepath, func):
    # Processing the desired data implementing the desired function
    func(cur, conn, filepath)


def main():
    try:
        conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
        
    except psycopg2.Error as err:
        print(f"error connecting to the database: {err}")
        
    try:
        cur = conn.cursor()
    
    except psycopg2.Error as err:
        print(f"error getting a cursor to the database: {err}")


    process_data(cur, conn, filepath='data/song_data', func=process_songs_file)
    process_data(cur, conn, filepath='data/log_data', func=process_logs_file)

    conn.close()


if __name__ == "__main__":
    main()