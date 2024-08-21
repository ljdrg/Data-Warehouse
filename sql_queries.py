import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN      = config.get("IAM_ROLE", "ARN")

LOG_DATA           = config.get("S3", "LOG_DATA")
LOG_JSONPATH       = config.get("S3", "LOG_JSONPATH")
SONG_DATA          = config.get("S3", "SONG_DATA")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events
                            (event_id bigint identity(0,1) PRIMARY KEY NOT NULL,
                            artist varchar,
                            auth varchar,
                            firstName varchar,
                            gender varchar,
                            itemInSession int, 
                            lastName varchar,
                            length numeric,
                            level varchar,
                            location varchar,
                            method varchar,
                            page varchar, 
                            registration numeric,
                            sessionId int,
                            song varchar,
                            status int,
                            ts timestamp,
                            userAgent varchar,
                            userId varchar
                            );
""")


staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs
                     (song_id varchar PRIMARY KEY NOT NULL,
                     num_songs int,
                     title varchar,
                     artist_name varchar,
                     artist_latitude varchar,
                     year int,
                     duration numeric,
                     artist_id varchar, 
                     artist_longitude numeric,
                     artist_location varchar);
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplay 
                         (songplay_id bigint identity(0,1) PRIMARY KEY NOT NULL,
                         start_time timestamp,
                         user_id varchar,
                         level varchar,
                         song_id varchar,
                         artist_id varchar,
                         session_id int,
                         location varchar,
                         user_agent varchar);
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users
                     (user_id varchar PRIMARY KEY NOT NULL, 
                     first_name varchar,
                     last_name varchar,
                     gender varchar, 
                     level varchar);
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs
                     (song_id varchar PRIMARY KEY NOT NULL,
                     title varchar,
                     artist_id varchar, 
                     year int,
                     duration numeric);
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists
                       (artist_id varchar PRIMARY KEY NOT NULL,
                       name varchar,
                       location varchar,
                       latitude numeric,
                       longitude numeric);
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time
                     (start_time timestamp PRIMARY KEY NOT NULL,
                     hour int,
                     day int,
                     week int,
                     month int,
                     year int,
                     weekday int);
""")

# STAGING TABLES

staging_events_copy = ("""copy staging_events from {}
credentials 'aws_iam_role={}'
format as json {}
region 'us-west-2'
TIMEFORMAT as 'epochmillisecs';
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""copy staging_songs from {}
credentials 'aws_iam_role={}'
format as json 'auto'
COMPUPDATE OFF region 'us-west-2'
TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
""").format(SONG_DATA, ARN)


# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplay
                         (
                         start_time,
                         user_id,
                         level,
                         song_id,
                         artist_id,
                         session_id,
                         location,
                         user_agent)
                         SELECT 
                         DISTINCT se.ts   AS start_time,
                         se.userID        AS user_id,
                         se.level         AS level,
                         ss.song_id       AS song_id,
                         ss.artist_id     AS artist_id,
                         se.sessionId     AS session_id,
                         se.location      AS location,
                         se.userAgent     AS user_agent

                         FROM staging_events AS se
                         JOIN staging_songs AS ss 
                         ON (ss.title=se.song AND se.artist=ss.artist_name)
                         WHERE se.page='NextSong';
""")

user_table_insert = ("""INSERT INTO users
                     (user_id, 
                     first_name,
                     last_name,
                     gender, 
                     level)
                     SELECT
                     DISTINCT se.userID AS user_id,
                     se.firstNAME AS first_name,
                     se.lastName AS last_name,
                     se.gender AS gender,
                     se.level AS level
                     FROM staging_events AS se
                     WHERE se.page='NextSong'
                     ;
                     
""")

song_table_insert = ("""INSERT INTO songs
                     (song_id,
                     title,
                     artist_id, 
                     year,
                     duration)
                     SELECT 
                     DISTINCT ss.song_id AS song_id,
                     ss.title AS title,
                     ss.artist_id AS artist_id,
                     ss.year AS year,
                     ss.duration AS duration
                     FROM staging_songs AS ss;
""")

artist_table_insert = ("""INSERT INTO artists
                       (artist_id,
                       name,
                       location,
                       latitude,
                       longitude)
                       SELECT 
                       DISTINCT ss.artist_id AS aritst_id,
                       ss.artist_name AS name,
                       ss.artist_location AS location,
                       ss.artist_latitude AS latitude,
                       ss.artist_longitude AS longitude
                       FROM staging_songs AS ss;
""")

time_table_insert = ("""INSERT INTO time
                     (start_time,
                     hour,
                     day,
                     week,
                     month,
                     year,
                     weekday)
                     SELECT
                     DISTINCT se.ts     AS start_time,
                     EXTRACT(hour FROM start_time) AS hour,
                     EXTRACT(day FROM start_time) AS day,
                     EXTRACT(week FROM start_time) AS week, 
                     EXTRACT(month FROM start_time) AS month,
                     EXTRACT(year FROM start_time) AS year,
                     EXTRACT(dow FROM start_time) AS weekday
                     FROM staging_events AS se
                     WHERE se.page = 'NextPage';
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
