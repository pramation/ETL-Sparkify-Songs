import configparser


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

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (songplay_id  varchar(100), 
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

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
