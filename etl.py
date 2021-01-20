import os
import sys
import glob
import logging
import psycopg2
import pandas as pd
from sql_queries import *
from typing import Callable
import psycopg2.extensions as psycopg2Ext

logger = logging.getLogger(__name__)


def process_song_file(cur: psycopg2Ext.cursor, filepath: str) -> None:
    """
    Description: This method reads each song file to get the artist and
        song observations and populate the `artists` and `songs` dim
        tables.

    Arguments:
        cur (psycopg2Ext.cursor): the cursor object.
        filepath (str): log data file path.

    Returns:
        None
    """
    # open song file
    try:
        df = pd.read_json(filepath, lines=True)
    except Exception:
        msg = f"Error: Could not read JSON content of {filepath}"
        logger.warning(msg)
        return

    # extract artist record from df
    try:
        artist_data = (
            df[
                [
                    "artist_id",
                    "artist_name",
                    "artist_location",
                    "artist_latitude",
                    "artist_longitude",
                ]
            ]
            .values[0]
            .tolist()
        )
    except Exception:
        msg = "Error: Could not extract artist columns from df"
        logger.warning(msg)
        return

    # insert artist record into artist table
    try:
        cur.execute(artist_table_insert, artist_data)
    except psycopg2.Error as e:
        msg = f"Error: Could not insert artist record in artist table"
        logger.warning(msg, e)
        return

    # extract song record from df
    try:
        song_data = (
            df[["song_id", "title", "artist_id", "year", "duration"]].values[0].tolist()
        )
    except Exception:
        msg = "Error: Could not extract song columns from df"
        logger.warning(msg)
        return

    # insert song record into song table
    try:
        cur.execute(song_table_insert, song_data)
    except psycopg2.Error as e:
        msg = f"Error: Could not insert song record in song table"
        logger.warning(msg, e)
        return


def process_log_file(cur: psycopg2Ext.cursor, filepath: str) -> None:
    """
    Description: This method reads each log file to extract the
        time and user observations and populate the `time` and
        `users` dim tables. It then, queries `song_id` and
        `artist_id` from `songs` and `artists` tables to insert
        with other relevant information into the `songplays`
        fact table.

    Arguments:
        cur (psycopg2Ext.cursor): the cursor object.
        filepath (str): log data file path.

    Returns:
        None
    """
    # open log file
    try:
        df = pd.read_json(filepath, lines=True)
    except Exception:
        msg = f"Error: Could not read JSON content of {filepath}"
        logger.warning(msg)
        return

    # convert timestamp column to datetime
    df["ts"] = pd.to_datetime(df["ts"], unit="ms")

    # filter by specific page action
    page = "NextSong"
    t = df[df["page"] == page]

    # insert time data records
    time_data = (
        t.ts,
        t.ts.dt.hour,
        t.ts.dt.isocalendar().day,
        t.ts.dt.isocalendar().week,
        t.ts.dt.month,
        t.ts.dt.isocalendar().year,
        t.ts.dt.weekday,
    )
    column_labels = ("start_time", "hour", "day", "week", "month", "year", "weekday")
    data = {column_labels[i]: time_data[i] for i in range(len(column_labels))}
    time_df = pd.DataFrame(data)

    # insert time records into time table
    for i, row in time_df.iterrows():
        try:
            cur.execute(time_table_insert, list(row))
        except psycopg2.Error as e:
            msg = f"Error: Could not insert following record in time table: {row}"
            logger.warning(msg, e)
            continue

    # load user table
    try:
        user_df = df[["userId", "firstName", "lastName", "gender", "level"]]
    except Exception:
        msg = "Error: Could not extract song columns from dataframe"
        logger.warning(msg, sys.exc_info()[0])
        return
    # filter user_df data if userId is not an empty string
    try:
        user_df = user_df[user_df["userId"] != ""]
    except Exception:
        msg = "Error: Could not drop empty string values from user_df.userId."
        logger.warning(msg)
        return

    # insert user records
    for i, row in user_df.iterrows():
        try:
            cur.execute(user_table_insert, row)
        except psycopg2.Error as e:
            msg = f"Error: Could not insert following record in user table: {row}"
            logger.warning(msg, e)
            continue

    df.dropna(axis=0, subset=["artist", "song", "length"], inplace=True)

    # insert songplay records
    for index, row in df.iterrows():
        # get songid and artistid from song and artist tables
        try:
            cur.execute(song_select, (row.song, row.artist, row.length))
        except psycopg2.Error as e:
            msg = f"Error: Could not get song_id and artist_id \
                from song and artist tables"
            logger.warning(msg, e)
            continue

        results = cur.fetchone()
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (
            row.ts,
            row.userId,
            row.level,
            songid,
            artistid,
            row.sessionId,
            row.location,
            row.userAgent,
        )

        try:
            cur.execute(songplay_table_insert, songplay_data)
        except psycopg2.Error as e:
            msg = f"Error: Could not insert songplay record."
            logger.warning(msg, e)
            continue


def process_data(
    cur: psycopg2Ext.cursor,
    conn: psycopg2Ext.connection,
    filepath: str,
    func: Callable[[None], None],
) -> None:
    """
    Describe: This method collects all files under
        filepath and while iterating over these files
        it will call the `func` input method.

    Arguments:
        cur (psycopg2Ext.cursor): the cursor object.
        conn (psycopg2Ext.connection): the connection object.
        filepath (str): data file path.
        func (Callable[[None], None])): method which returns None.

    Returns:
        None
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, "*.json"))
        for f in files:
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print(f"{num_files} files found in {filepath}")

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print(f"{i}/{num_files} files processed.")


def main() -> None:
    """
    Description: This method establishes a connection with
        sparkifydb and calls `process_data` method on song
        and log files.

    Returns:
        None
    """
    try:
        conn = psycopg2.connect(
            "host=127.0.0.1 dbname=sparkifydb user=student password=student"
        )
    except psycopg2.Error as e:
        msg = "ERROR: Could not make connection to sparkify database."
        logger.warning(msg, e)
        return

    try:
        cur = conn.cursor()
    except psycopg2.Error as e:
        msg = "ERROR: Could not get cursor to sparkify database."
        logger.warning(msg, e)
        return

    # process song files
    process_data(cur, conn, filepath="data/song_data", func=process_song_file)

    # process log files
    process_data(cur, conn, filepath="data/log_data", func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
