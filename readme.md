# Data Modeling with PostgreSQL

## Introduction

In this project, we will create a Postgre database which will host data collected on songs and user activity on Sparkify's new music streaming app. First we will start by creating the database schema during which process we will define _fact_ and _dimension_ tables for a _star_ schema. Then, we will create ETL pipelines which will transfer data from files in local directories into the tables in Postgre using both Python and SQL. Finally, we will conclude by testing the database and ETL pipelines using provided tests from the analytics team.

### Setup Environment

In order to run any of the files associated with this project, we need to create an Anaconda environment. Make sure you have an updated version of [Anaconda](https://anaconda.org/). Follow these steps on your terminal to replicate the environment:

1. `conda create -n project1 python=3.9 pandas psycopg2 ipykernel`. When asked `"Proceed ([y]/n)?"`, make sure to respond with **y**.
2. `source activate project1`

If running the `etl.ipynb` notebook, make sure to also run this on your terminal:

```bash
python -m ipykernel install --user --name project1
```

Now we are ready to proceed with our project.

### Data Overview

We will combine data from two local directories to create the _fact_ and _dimension_ tables.

The first dataset contains metadata about a song and the artist of that song which is saved in a JSON format. These files are partitioned by the first three letters of each song's track ID and are located under `./data/song_data/`. A single song file has the following information:
- `num_songs` - number of songs by artist
- `artist_id` - artist id
- `artist_latitude` - latitude coordinate if provided which depends on the artist's location
- `artist_longitude` - longitude coordinate if provided which depends on the artist's location
- `artist_location` - address or location of artist
- `artist` - full name of artist
- `song_id` - id used to identify song
- `title` - title of song
- `duration` - duration of song
- `year` - year when song was published

The second dataset contains log files generated by an event simulator based on the songs in the first dataset. This event simulation, simulates activity logs from the Sparkify music streaming app. These log files are partitioned by year and month and are located under `./data/log_data/`. These files contain the following information:
- `artist` - name of artist
- `auth` - tracks whether the user logged in or logged out
- `firstName` - user first name
- `lastName` - user last name
- `gender` - user gender
- `length` - length of session/event
- `itemInSession` - number of items for a specific session
- `level` - tracks whether the user paid for the session or if the session was free
- `location` - user location
- `method` - HTTP methods
- `page` - tracks page name such as 'NextSong', 'Home', 'Logout', 'Settings', 'Downgrade', 'Login', 'Help', 'Error', 'Upgrade'
- `registration` - registration timestamp
- `sessionId` - session id
- `song` - song name
- `status` - tracks the status of the request such as status 200, 307, 404
- `ts` - timestamp in millisecond
- `userAgent` - operating system user agent
- `userId` - user id

## Schema Design

### Fact Table

We will use the song and log datasets to create a _star_ schema optimized for queries on song play analysis. We will create a _fact table_ which we will refer to as `songplays`. This table will record log data associated with song plays and will include the following field names, respective field types and any additional field attributes:

| field name | field type | field attribute(s)  |
| ----------- | ----------- | ------------ |
| songplay_id | SERIAL | PRIMARY KEY |
| start_time  | TIMESTAMP | FOREIGN KEY |
| user_id     | INT | FOREIGN KEY |
| level       | VARCHAR | |
| song_id     | TEXT |  |
| artist_id   | TEXT |  |
| session_id  | INT | NOT NULL |
| location    | TEXT | |
| user_agent  | TEXT | |

Specifying a `PRIMARY KEY` attribute means that we are defining a field that uniquely identifies every different record in the table. A `FOREIGN KEY` attribute means the field uniquely identifies every different record in a different table where it also is a `PRIMARY KEY`. Lastly, a `NOT NULL` constraint makes the specified field required.

Because the script provided for this project assumes that the `artist_id` and `song_id` could take `None` values, we will not label these two fields as `FOREIGN KEY` nor will we add a field restriction such as `NOT NULL`. That said, in other applications, we would want to add a `FOREIGN KEY` attribute on the two fields to make sure all our tables are up-to-date.

### Dimension Tables

We will create four additional dimension tables: `users`, `songs`, `artists`, and `time`. They each contain the following field names, types, and attributes:

***Table: `users`***
| field name | field type | field attribute(s)  |
| ----------- | ----------- | --------------- |
| user_id | INT | PRIMARY KEY |
| first_name | TEXT | |
| last_name | TEXT | |
| gender | VARCHAR | |
| level | VARCHAR | |

***Table: `songs`***
| field name | field type | field attribute(s)  |
| ----------- | ----------- | --------------- |
| song_id | TEXT | PRIMARY KEY |
| title |  TEXT | NOT NULL |
| artist_id | TEXT | FOREIGN KEY |
| year | INT | |
| duration | FLOAT | |

***Table: `artists`***
| field name | field type | field attribute(s)  |
| ----------- | ----------- | --------------- |
| artist_id | TEXT | PRIMARY KEY |
| name | TEXT | NOT NULL |
| location | TEXT | |
| latitude | FLOAT | |
| longitude | FLOAT | |

***Table: `time`***
| field name | field type | field attribute(s)  |
| ----------- | ----------- | --------------- |
| start_time | TIMESTAMP | PRIMARY KEY |
| hour | INT | |
| day | INT | |
| week | INT | |
| month | INT | |
| year | INT | |
| weekday | INT | |

#### Visual Representation

![star_schema_design](./image1.jpeg)


### Creating Tables

In order to create the tables, we must first look at `create_tables.py` file which will first delete the `sparkify` database if it exists and will re/create it. Then, it will establish a connection with the `sparkify` database and get the cursor to it. It will delete all/any tables if they exist and create all tables needed. Finally, it will close the connection.

The queries to drop and create the tables are imported from `sql_queries.py`, so we will complete this file first. The `sql_queries.py` file contains all the needed queries for creating and dropping tables, inserting records, and finding songs. We can use the SQL syntax for dropping tables which looks like `"DROP TABLE IF EXISTS <name>"` where `<name>` will be replaced with the appropriate name of each table we are going to drop.

To create the tables, we follow this simple syntax `"CREATE TABLE IF NOT EXISTS <name> (<fieldName1 type>, <fieldName2 type>, ..., <fieldAttribute1>, <fieldAttribute2>, ...);"`. We will replace `<name>` with the specific table name and `<fieldName# type>` with all the respective table fields and types. A primary key attribute can be either added right after the field name and type have been declared followed by `PRIMARY KEY`, or as an attribute which will replace `<fieldAttribute#>`. Additionally, we will add foreign key attributes following this syntax: `"FOREIGN KEY (<fieldName>) REFERENCES <name> (<fieldName>));"`.

This is an example of a query utilizing primary and foreign key attributes:

```sql
CREATE TABLE IF NOT EXISTS songs (
    song_id text,
    title text NOT NULL,
    artist_id text,
    year int,
    duration float,
    PRIMARY KEY (song_id),
    FOREIGN KEY (artist_id) REFERENCES artists (artist_id)
);
```

To insert records, we follow a similar query syntax `"INSERT INTO <name> (<fieldName1>, <fieldName2>, ...) VALUES (%s, %s, ...)"`.


_Note_: Since we are using `PRIMARY KEY` and `FOREIGN KEY` field attributes, the order in which we generate the tables matters. This means that the `create_table_queries` list has been reorder accordingly, like so:

```python
# sql_queries.py
create_table_queries = [user_table_create, artist_table_create, time_table_create, song_table_create, songplay_table_create]
```

After completing the appropriate queries, in order to execute them, we run `create_tables.py` on terminal, like so:

```bash
python create_tables.py
```

_Note_: Make sure you have followed the steps described in the _Setup Environment_ section. The `logging` package utilizes syntax compatible with the latest version of Python.

### Testing Created Tables

To make sure that tables were created successfully, we will run the `test.ipynb` notebook. First, we will make sure that we have closed all connections to `sparkifydb`; otherwise, we will not be able to connect through the `test.ipynb` notebook.

While running each notebook cell, you should expect the connection to `student@sparkifydb` to be successful and each query should display the appropriate fields for each table with the following information: `"0 rows affected."`. You will notice that the tables are empty, which is expected since we have not inserted any data.

## ETL Pipelines

In this section, we will develop pipelines for extracting, transforming, loading (ETL) the data into the tables. We will start by programmatically iterating over the files under `./data/song_data/`. We will use the following method to collect all the files we need to process in a list

```python
def get_files(filepath):
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    return all_files
```

Since we have added foreign key field attributes to some of the tables we would like to create, the order in which we process the ETLs matters. This is the main motivation why we start with the `artists` table.

### Populate `artists` Dimension Table

During the process of iterating over the full list of files found under `./data/song_data/`, we will read each file content and extract only the relevant column headers from that data. For instance, if we save the content of a file under `song_file` variable, we will load the content using the pandas `read_json` method, like so

```python
df = pd.read_json(song_file, lines=True)
```

We already know, by the way the source data has been collected and saved into JSON files that each file contains one record. This means that when we read each file, we are interested in extracting the first and only record. Thus, the only filtering we need to do is to extract the relevant column headers. We need to be cautious to make sure that the order in which we extract each column header matches the order in which we insert elements into the Postgre tables.

To extract the relevant column headers and the first/only record in the dataframe, and insert entry into the `artists` table, we run

```python
# extract relevant column headers and first/only entry in dataframe
artist_data = df[["artist_id", "artist", "artist_location", "artist_latitude", "artist_longitude"]].values[0].tolist()
# add entry in `artists` table
cur.execute(artist_table_insert, artist_data)
conn.commit()
```

**Note on SQL syntax for inserting artists entries**:

The syntax used to insert entries into the `artists` table looks like this:

```sql
INSERT INTO artists (
    artist_id,
    name,
    location,
    latitude,
    longitude
) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (artist_id) DO NOTHING;
```

The reason we have added the `"ON CONFLICT DO NOTHING"` constraint is because the files might contain duplicate artist names which might have already been entered in the database. Thus, in the event that the entry (more specifically, `artist_id` key) already exists in the `artists` table, we will continue on to the next entry. This will allow `etl.py` to execute with no "duplicate" warnings.


### Populate `songs` Dimension Table

Similarly to the steps we followed above, we need to extract some additional information from the same JSON files to insert into the `songs` dimension table. This is what this extraction and loading looks like for the `songs` table:

```python
# extract relevant column headers and first/only entry in dataframe
song_data = df[["song_id", "title", "artist_id", "year", "duration"]].values[0].tolist()
# add entry in `songs` table
cur.execute(song_table_insert, song_data)
conn.commit()
```

**Note on SQL syntax for inserting song entries**:

The syntax used to insert entries into the `songs` table looks like this:

```sql
INSERT INTO songs (
    song_id,
    title,
    artist_id,
    year,
    duration
) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (song_id) DO NOTHING;
```

Similarly to above, the reason we have added the `"ON CONFLICT DO NOTHING"` constraint is because the files might contain duplicate song information which might have already been entered in the database. In the event that the entry (more specifically, `song_id` key) already exists in the `songs` table, we will continue on to the next entry. This will allow `etl.py` to execute with no duplicate warnings.

### Populate `time` Dimension Table

To create the rest of the dimension tables and fact table, we will use log data found under `./data/log_data/`. After extracting the full list of log files, we will iterate over them to extract specific columns to populate the `time` table. We will use panda's `read_json` method to load the content. First, we need to convert the `ts` timestamp column - provided in milliseconds - to datetime. This can be easily done, assuming the read data is saved in a `df` dataframe, by running

```python
df["ts"] = pd.to_datetime(df["ts"], unit="ms")
```

Next, we filter the data by `"NextSong"` page action information

```python
# Filter by "NextSong" page
t = df[ df["page"] == "NextSong"]
```

We will extract the timestamp, hour, day, week of year, month, year, and weekday from the `ts` column and set `time_data` to a list containing these values in order

```python
# Get timestamp, hour, day, week of year, month, year, and weekday
time_data = (
    t.ts, t.ts.dt.hour,
    t.ts.dt.isocalendar().day, t.ts.dt.isocalendar().week,
    t.ts.dt.month, t.ts.dt.isocalendar().year,
    t.ts.dt.weekday
)
```

We will also specify the labels for these columns under `column_labels` and create a dataframe, `time_df`, containing the data for this file by combining `column_labels` and `time_data` into a dictionary and converting this into a dataframe

```python
column_labels = ("start_time", "hour", "day", "week", "month", "year", "weekday")

# combine time_data and column_labels
data = {column_labels[i]:time_data[i] for i in range(len(column_labels))}

# create dataframe from dictionary
time_df = pd.DataFrame(data)
```

Finally, we will insert each entry of the dataframe info the `time` table, like so

```python
for i, row in time_df.iterrows():
    cur.execute(time_table_insert, list(row))
    conn.commit()
```


**Note on SQL syntax for inserting time entries**:

The syntax used to insert entries into the `time` table looks like this:

```sql
INSERT INTO time (
    start_time,
    hour,
    day,
    week,
    month,
    year,
    weekday
) VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (start_time) DO NOTHING;
```

### Populate `users` Dimension Table

We will extract the user entries from the `df` dataframe.

```python
# extract relevant column headers and first/only entry in dataframe
user_df = df[["userId", "firstName", "lastName", "gender", "level"]]

# remove nonempty string from user_df
user_df = user_df[user_df["userId"] != ""]

# add entry in `users` table
for i, row in user_df.iterrows():
    cur.execute(user_table_insert, row)
```

**Note on SQL syntax for inserting user entries**:

The syntax used to insert entries into the `songs` table looks like this:

```sql
INSERT INTO users (
    user_id,
    first_name,
    last_name,
    gender,
    level
) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (user_id) DO UPDATE SET
   (first_name, last_name, gender, level)=
   (EXCLUDED.first_name , EXCLUDED.last_name , EXCLUDED.gender, EXCLUDED.level);
```

Note that unlike the other queries, the above query updates the row if the user's id already exists in the table. This is done primarily because the `level` field can be updated; a user could decide to upgrade from a free service to a paid one eventually, or the other way around. To ensure high quality during the insertion process, we need to make sure that the files are sorted in chronological order during the read-and-write iteration, so that the information being inserted is the most recent one. In this project, this has already been taken care of, but it is important to keep in mind nonetheless.


### Populate `songplays` Fact Table

In order to populate the `songplays` table, we must first combine data from the `songs` and `artists` tables to find the respective `song_id` and `artist_id` fields. This can be easily done by joining the two tables using the `artist_id` field. We will iterate over the `df` dataframe and for each row's `artist`, `song`, `length` we will try to find a match on the joined tables.

To minimize any query errors, we will first remove any rows in the dataframe if either `artist`, `song`, or `length` values are `None`

```python
df.dropna(axis=0, subset=["artist", "song", "length"], inplace=True)
```

Next, let's implement the `song_select` query in `sql_queries.py` to find the `song_id` and `artist_id` based on the title, artist name, and duration of song.

```sql
SELECT songs.song_id, artists.artist_id FROM \
songs JOIN artists ON songs.artist_id=artists.artist_id \
WHERE artists.name=%s AND songs.title=%s AND \
songs.duration=%s;
```

In the event that the row in `df` is not matched with data in the joined table, we will still insert this data in `songplays` table and we will equate `song_id` and `artist_id` with the value `None`. Now, let's extract the `songplays` entries:

```python
songplay_data = (
    row.ts, row.userId, row.level,
    songid, artistid, row.sessionId,
    row.location, row.userAgent
)
```

#### Insert Records into Songplays Table

- Implement the `songplay_table_insert` query and run the cell below to insert records for the songplay actions in this log file into the `songplays` table. Remember to run `create_tables.py` before running the cell below to ensure you've created/resetted the `songplays` table in the sparkify database.

**Note on SQL syntax for inserting songplay entries**:

The following is the implementation of the `songplay_table_insert` query syntax for inserting entries into the `songplays` table:

```sql
INSERT INTO songplays (
    start_time,
    user_id,
    level,
    song_id,
    artist_id,
    session_id,
    location,
    user_agent
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
```

Note that the actual table includes an additional field named `songplay_id` of type `SERIAL`. This means that this data type allows us to automatically generate unique integer numbers for the `songplay_id`. Therefore, it is not included in the insert statement above.

### Execute ETL Pipeline

Finally, to execute all the ETL pipelines, run the following on your command line:

```bash
python etl.py
```

#### Error Handling

Both, the `create_tables.py` and `etl.py` use `logging` to document any exception handling on the terminal. If the script runs into any errors, the warning will be displayed on the user's terminal and will either terminate the program or continue to the next iteration. See both files for detailed information.

### Testing ETL Pipelines

Now that we have populated all tables, we can run simple tests to make sure each table is not empty. The following Jupyter Notebook has already been provided `test.ipynb`. The notebook queries each table by first displaying the first 5 entries in the table and then counting the total number of observations for each table.

## Authors

Yuna Luzi - @najuzilu
