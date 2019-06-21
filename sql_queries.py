import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= (
    """CREATE TABLE IF NOT EXISTS staging_events 
    (artist VARCHAR(225), 
    auth VARCHAR(25), 
    first_name VARCHAR(25), 
    gender VARCHAR(2), 
    item_in_session INTEGER, 
    last_name VARCHAR(25), 
    length DECIMAL(8,2), 
    level VARCHAR(25), 
    location VARCHAR(225), 
    method VARCHAR(25), 
    page VARCHAR(25), 
    registration BIGINT, 
    session_id INTEGER, 
    song VARCHAR(225), 
    status INTEGER, 
    ts BIGINT, 
    user_agent VARCHAR(225), 
    user_id INTEGER)"""
)

staging_songs_table_create = (
    """CREATE TABLE IF NOT EXISTS staging_songs
    (num_songs INTEGER, 
    artist_id VARCHAR(25), 
    artist_latitude DECIMAL(8,2),
    artist_longitude DECIMAL(8,2),
    artist_location VARCHAR(225),
    artist_name VARCHAR(225),
    song_id VARCHAR(25),
    title VARCHAR(225),
    duration DECIMAL(8,2),
    year INTEGER )"""
)

songplay_table_create = (
    """CREATE TABLE IF NOT EXISTS songplays
    (songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY,
    start_time BIGINT NOT NULL sortkey, 
    user_id INTEGER NOT NULL, 
    level VARCHAR(25), 
    song_id VARCHAR(25) distkey, 
    artist_id VARCHAR(25), 
    session_id INTEGER, 
    location VARCHAR(225), 
    user_agent VARCHAR(225))"""
)

user_table_create = (
    """CREATE TABLE IF NOT EXISTS users
    (user_id INTEGER PRIMARY KEY sortkey,
    first_name VARCHAR(25),
    last_name VARCHAR(25),
    gender VARCHAR(25),
    level VARCHAR(25))
    diststyle all;"""
)

song_table_create = (
    """CREATE TABLE IF NOT EXISTS songs 
    (song_id VARCHAR(25) PRIMARY KEY sortkey distkey, 
    title VARCHAR(225), 
    artist_id VARCHAR(25) NOT NULL,
    year INTEGER,
    duration DECIMAL(8,2))"""
)

artist_table_create = (
    """CREATE TABLE IF NOT EXISTS artists 
    (artist_id VARCHAR(25) PRIMARY KEY sortkey, 
    name VARCHAR(225), 
    location VARCHAR(225), 
    latitude DECIMAL(8,2), 
    longitude DECIMAL(8,2))"""
)

time_table_create = (
    """CREATE TABLE IF NOT EXISTS time 
    (start_time BIGINT PRIMARY KEY sortkey, 
    hour INTEGER, 
    day INTEGER, 
    week INTEGER, 
    month INTEGER, 
    year INTEGER, 
    weekday INTEGER)"""
)

# STAGING TABLES

staging_events_copy = (
    """copy staging_events from {}
     credentials 'aws_iam_role={}'
     region 'us-west-2'
     json {};
    """).format(config.get('S3','LOG_DATA'), config.get('IAM_ROLE','ARN'), config.get('S3','LOG_JSONPATH'))


staging_songs_copy = (
     """copy staging_songs from {}
     credentials 'aws_iam_role={}'
     region 'us-west-2'
     json 'auto';
    """).format(config.get('S3','SONG_DATA'), config.get('IAM_ROLE','ARN'))

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, 
    song_id, artist_id, session_id, location, user_agent) 
    SELECT e.ts AS start_time,
           e.user_id AS user_id,
           e.level AS level,
           s.song_id AS song_id,
           s.artist_id AS artist_id,
           e.session_id AS session_id,
           e.location AS location,
           e.user_agent AS user_agent
    FROM staging_events e
    LEFT OUTER JOIN staging_songs s ON (e.song=s.title AND e.artist=s.artist_name)
    WHERE e.page='NextSong';
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT user_id AS user_id,
           first_name       AS first_name,
           last_name        AS last_name,
           gender           AS gender,
           level            AS level
    FROM staging_events
    WHERE user_id IS NOT NULL;
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT song_id          AS song_id,
           title                     AS title,
           artist_id                 AS artist_id,
           year                      AS year,
           duration                  AS duration
    FROM staging_songs
    WHERE song_id IS NOT NULL and artist_id IS NOT NULL;
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT artist_id           AS artist_id,
           artist_name                  AS name,
           artist_location              AS location,
           artist_latitude              AS latitude,
           artist_longitude             AS longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT ts AS start_time,
         EXTRACT(hour FROM dateadd(second, ts/1000 + 8*60*60, '19700101') ) AS hour,
           EXTRACT(day FROM dateadd(second, ts/1000 + 8*60*60, '19700101') ) AS day,
           EXTRACT(week FROM dateadd(second, ts/1000 + 8*60*60, '19700101') ) AS week,
           EXTRACT(month FROM dateadd(second, ts/1000 + 8*60*60, '19700101'))  AS month,
           EXTRACT(year FROM dateadd(second, ts/1000 + 8*60*60, '19700101'))  AS year,
           EXTRACT(weekday FROM dateadd(second, ts/1000 + 8*60*60, '19700101'))  AS weekday
    FROM staging_events;       
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
