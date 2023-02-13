import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

LOG_DATA  = config.get("S3", "LOG_DATA")
LOG_PATH  = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")
IAM_ROLE  = config.get("IAM_ROLE","ARN")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
    artist           varchar(256),
    auth             varchar(20),
    first_name       varchar(50),
    gender           char(1),
    item_in_session  int,
    last_name        varchar(50),
    length           decimal(10, 5),
    level            varchar(10),
    location         varchar(256),
    method           varchar(10),
    page             varchar(50),
    registration     decimal(14, 1),
    session_id       int,
    song             varchar,
    status           int,
    ts               bigint,
    user_agent       varchar,
    user_id          int
)
diststyle auto
sortkey auto;
""")

staging_songs_table_create =("""
CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs           int,
    artist_id           varchar(50),
    artist_latitude     decimal(9, 6),
    artist_longitude    decimal(9, 6),
    artist_location     varchar(256),
    artist_name         varchar(256),
    song_id             varchar(50),
    title               varchar(256),
    duration            decimal(10, 5),
    year                int
)
diststyle auto
sortkey auto;
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (
    songplay_id int IDENTITY(0, 1) PRIMARY KEY,
    start_time timestamp NOT NULL,
    user_id int NOT NULL,
    level varchar,
    song_id varchar,
    artist_id varchar,
    session_id int NOT NULL,
    location varchar,
    user_agent varchar
);
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (
    user_id varchar PRIMARY KEY,
    first_name varchar,
    last_name varchar,
    gender varchar,
    level varchar
);
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (
    song_id varchar PRIMARY KEY,
    title varchar,
    artist_id varchar NOT NULL,
    year int,
    duration numeric
);
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (
    artist_id varchar PRIMARY KEY,
    name varchar,
    location varchar,
    latitude numeric,
    longitude numeric
);
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (
    start_time timestamp PRIMARY KEY,
    hour int,
    day int,
    week int,
    month int,
    year int,
    weekday int
);
""")

# STAGING TABLES

staging_events_copy = ("""
copy staging_events from {bucket}
    credentials 'aws_iam_role={role}'
    region      'us-west-2'
    format       as JSON {path}
    timeformat   as 'epochmillisecs'
""").format(bucket=LOG_DATA, role=IAM_ROLE, path=LOG_PATH)

staging_songs_copy = ("""copy staging_songs from {bucket}
    credentials 'aws_iam_role={role}'
    region      'us-west-2'
    format       as JSON 'auto'
""").format(bucket=SONG_DATA, role=IAM_ROLE)


# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT DISTINCT timestamp 'epoch' + ts / 1000 * interval '1 second' AS start_time,
                user_id,
                level,
                song_id,
                artist_id,
                session_id,
                location,
                user_agent
           FROM staging_events, staging_songs
          WHERE staging_events.song = staging_songs.title
            AND staging_events.artist = staging_songs.artist_name
            AND user_id IS NOT NULL
            AND song_id IS NOT NULL
            AND artist_id IS NOT NULL
            AND ts IS NOT NULL
            AND staging_events.length IS NOT NULL
            AND staging_events.length = staging_songs.duration
            AND staging_events.page = 'NextSong';
""")

user_table_insert = ("""INSERT INTO users SELECT DISTINCT (user_id)
        user_id,
        first_name,
        last_name,
        gender,
        level
        FROM staging_events
        WHERE user_id IS NOT NULL 
        AND first_name IS NOT NULL
        AND last_name IS NOT NULL
""")

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT song_id, title, artist_id, year, duration
           FROM staging_songs
          WHERE song_id IS NOT NULL
            AND title IS NOT NULL
            AND duration IS NOT NULL;
""")

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
           FROM staging_songs
          WHERE artist_id IS NOT NULL
            AND artist_name IS NOT NULL;
""")

time_table_insert = (""" INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT timestamp 'epoch' + ts / 1000 * interval '1 second' AS start_time,
                extract(hour from (timestamp 'epoch' + ts / 1000 * interval '1 second')),
                extract(day from (timestamp 'epoch' + ts / 1000 * interval '1 second')),
                extract(week from (timestamp 'epoch' + ts / 1000 * interval '1 second')),
                extract(month from (timestamp 'epoch' + ts / 1000 * interval '1 second')),
                extract(year from (timestamp 'epoch' + ts / 1000 * interval '1 second')),
                extract(weekday from (timestamp 'epoch' + ts / 1000 * interval '1 second'))
           FROM staging_events
          WHERE ts IS NOT NULL;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
