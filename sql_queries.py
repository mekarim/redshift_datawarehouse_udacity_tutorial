import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')


# DROP TABLES
staging_events_table_drop = '''DROP TABLE IF EXISTS staging_events;'''
staging_songs_table_drop = '''DROP TABLE IF EXISTS staging_songs;'''
songplay_table_drop = '''DROP TABLE IF EXISTS songplays;'''
user_table_drop = '''DROP TABLE IF EXISTS users;'''
song_table_drop = '''DROP TABLE IF EXISTS songs;'''
artist_table_drop = '''DROP TABLE IF EXISTS artists;'''
time_table_drop = '''DROP TABLE IF EXISTS time;'''


# CREATE TABLES
staging_events_table_create = ''' 
                                     CREATE TABLE IF NOT EXISTS "staging_events" (
                                          "artist" VARCHAR,
                                          "auth" VARCHAR,
                                          "firstName" VARCHAR,
                                          "gender" VARCHAR,
                                          "itemInSession" INT,
                                          "lastName" VARCHAR,
                                          "length" DECIMAL(8, 2),
                                          "level" VARCHAR,
                                          "location" VARCHAR,
                                          "method" VARCHAR,
                                          "page" VARCHAR,
                                          "registration" FLOAT,
                                          "sessionId" INT,
                                          "song" VARCHAR,
                                          "status" INT,
                                          "ts" INT,
                                          "userAgent" VARCHAR,
                                          "userId" INT
                                     );
                              '''


staging_songs_table_create = ("""
                                     CREATE TABLE IF NOT EXISTS "staging_songs" (
                                          "song_id" VARCHAR,
                                          "num_songs" INT,
                                          "title" VARCHAR,
                                          "artist_name" VARCHAR,
                                          "artist_latitude" FLOAT,
                                          "year" INT,
                                          "duration" REAL,
                                          "artist_id" VARCHAR,
                                          "artist_longitude" FLOAT,
                                          "artist_location" VARCHAR
                                     );
                              """)


songplay_table_create = '''
                             CREATE TABLE IF NOT EXISTS "songplays" (
                                    songplay_id BIGINT IDENTITY(0, 1) PRIMARY KEY, 
                                    start_time TIMESTAMP, user_id INT, 
                                    level VARCHAR, song_id VARCHAR, artist_id VARCHAR,     
                                    session_id INT, location VARCHAR, user_agent VARCHAR
                             );
                         '''


user_table_create = '''
                        CREATE TABLE IF NOT EXISTS "users" (
                                user_id INT PRIMARY KEY, first_name VARCHAR, 
                                last_name VARCHAR, gender VARCHAR, level VARCHAR
                        );
                    '''


song_table_create = ("""
                        CREATE TABLE "songs" (
                            song_id VARCHAR PRIMARY KEY, title VARCHAR, 
                            artist_id VARCHAR, year INT, duration NUMERIC
                        );
                     """)


artist_table_create = ("""
                          CREATE TABLE IF NOT EXISTS "artists" (
                              artist_id VARCHAR PRIMARY KEY, name VARCHAR, 
                              location VARCHAR, latitude NUMERIC, longitude NUMERIC
                          );
                       """)


time_table_create = ("""
                        CREATE TABLE IF NOT EXISTS "time" (
                            start_time TIMESTAMP, 
                            hour INT, day INT, week INT, 
                            month INT, year INT, weekday INT
                        );
                     """)


# STAGING TABLES
staging_events_copy = ("""
""").format()

staging_songs_copy = ("""
""").format()


# FINAL TABLES
songplay_table_insert = ("""""")

user_table_insert = ("""""")

song_table_insert = ("""""")

artist_table_insert = ("""""")

time_table_insert = ("""""")


# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]