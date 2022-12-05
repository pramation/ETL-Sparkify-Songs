import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')


# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events (artist VARCHAR(500),
auth VARCHAR(50),
firstName VARCHAR(50),
gender CHAR(1),
itemInSession  INTEGER,
lastName VARCHAR(50),
length FLOAT(10),
level VARCHAR(20),
location VARCHAR(100),
method VARCHAR(10),
page VARCHAR(50),
registration FLOAT(20),
sessionId INTEGER,
song VARCHAR(500),
status INTEGER,
ts BIGINT,
userAgent VARCHAR(1000),
userId INTEGER);
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs(
num_songs INTEGER,
artist_id VARCHAR(50),
artist_latitude FLOAT(10),
artist_longitude FLOAT(10),
artist_location VARCHAR(1000),
artist_name VARCHAR(1000),
song_id varchar(50),
title varchar(1000),
duration FLOAT(10),
year INTEGER
);
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (songplay_id  bigint identity(0, 1) NOT NULL, 
start_time  DATE NOT NULL, 
user_id  INTEGER NOT NULL, 
level varchar(10) NOT NULL, 
song_id  varchar(50),
artist_id  varchar(50) NOT NULL,
session_id INTEGER,
location varchar(300),
user_agent  varchar(300),
FOREIGN KEY(song_id) REFERENCES songs(song_id),
FOREIGN KEY(user_id) REFERENCES users(user_id),
FOREIGN KEY(artist_id) REFERENCES artists(artist_id));
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (user_id integer PRIMARY KEY, 
first_name varchar(30) NOT NULL, 
last_name varchar(30) NOT NULL, 
gender char(1) NOT NULL, 
level varchar(10)
);
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (song_id varchar(50) PRIMARY KEY , 
title varchar(1000) NOT NULL, 
artist_id varchar(50) NOT NULL , 
year integer NOT NULL, 
duration float(10) NOT NULL);
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (artist_id varchar(50) PRIMARY KEY,
artist_name varchar(500) NOT NULL, 
artist_location varchar(1000),
artist_latitude FLOAT(10), 
artist_longitude FLOAT(10));
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (start_time timestamp NOT NULL, 
hour integer NOT NULL, 
day integer NOT NULL, 
week integer NOT NULL, 
month integer NOT NULL, 
year integer NOT NULL,
weekday integer NOT NULL);
""")

# STAGING TABLES

staging_events_copy = ("""copy {} from {} iam_role {} json {} compupdate off region 'us-west-2'
""").format('staging_events',config.get("S3", "LOG_DATA"), config.get("IAM_ROLE", "ARN"), config.get("S3", "LOG_JSONPATH"))

staging_songs_copy = ("""copy {} from {} iam_role {} json 'auto' compupdate off region 'us-west-2'
""").format('staging_songs',config.get("S3", "SONG_DATA"), config.get("IAM_ROLE", "ARN"))

# FINAL TABLES

songplay_table_insert = ("""INSERT into songplays( start_time ,user_id ,level, song_id,artist_id,session_id,location,user_agent) 
SELECT   TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second' as start_time ,se.userId ,se.level, ss.song_id,ss.artist_id,se.sessionId,se.location,se.userAgent
FROM staging_events se
JOIN staging_songs ss ON (ss.title=se.song)
WHERE se.page='NextSong' and ss.song_id IS NOT NULL  and se.userId is NOT NULL;
""")

user_table_insert = ("""INSERT into users (user_id, first_name, last_name, gender, level)
                        SELECT DISTINCT se.userId,se.firstName,se.lastName,se.gender,se.level FROM staging_events se 
                        WHERE se.userId IS  NOT NULL and se.firstName IS NOT NULL and se.lastName IS NOT NULL and se.gender IS NOT NULL;
                        
""")

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration)
                        SELECT DISTINCT ss.song_id,ss.title,ss.artist_id, ss.year, ss.duration from staging_songs ss
                        WHERE song_id IS NOT NULL and title is NOT NULL and year IS NOT NULL and duration is NOT NULL;
""")

artist_table_insert = ("""INSERT INTO artists (artist_id ,artist_name, artist_location,artist_latitude, artist_longitude)
                          SELECT DISTINCT ss.artist_id, ss.artist_name,ss.artist_location, ss.artist_latitude, ss.artist_longitude
                          FROM staging_songs ss
                          WHERE artist_id IS NOT NULL and artist_name IS NOT NULL;
""")

time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year,weekday)
                        SELECT DISTINCT start_time,EXTRACT(hour FROM start_time), EXTRACT(day FROM start_time), EXTRACT(week FROM start_time), EXTRACT(month FROM start_time),
                        EXTRACT(year FROM start_time), EXTRACT(weekday FROM start_time) FROM songplays;
                                          
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
#create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
