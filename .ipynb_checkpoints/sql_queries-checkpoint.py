# CONFIG
import configparser
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN = config.get("IAM_ROLE", "ARN")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")
LOG_DATA = config.get("S3", "LOG_DATA")

# DROP TABLES
staging_events_table_drop = '''DROP TABLE IF EXISTS staging_events;'''
staging_songs_table_drop = '''DROP TABLE IF EXISTS staging_songs;'''

songplay_table_drop = '''DROP TABLE IF EXISTS songplays;'''

artist_table_drop = '''DROP TABLE IF EXISTS artists;'''
user_table_drop = '''DROP TABLE IF EXISTS users;'''
song_table_drop = '''DROP TABLE IF EXISTS songs;'''
time_table_drop = '''DROP TABLE IF EXISTS time;'''


# CREATE TABLES
staging_events_table_create = (''' 
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
                                    "ts" BIGINT,
                                    "userAgent" VARCHAR,
                                    "userId" INT);
                              ''')


staging_songs_table_create = ('''
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
                                  "artist_location" VARCHAR);
                              ''')


# Fact Table
# Create songplays table which stores records in event data associated with song plays i.e. records with page NextSong
# Attributes: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
songplay_table_create = ('''
                            CREATE TABLE IF NOT EXISTS "songplays" (
                                songplay_id BIGINT IDENTITY(0, 1) PRIMARY KEY, 
                                start_time TIMESTAMP, user_id INT, 
                                level VARCHAR, song_id VARCHAR, artist_id VARCHAR,     
                                session_id INT, location VARCHAR, user_agent VARCHAR);
                        ''')


# Dimension Tables
# Create artists table which stores artists in music database.
# Attributes: artist_id, name, location, lattitude, longitude
artist_table_create = ('''
                          CREATE TABLE IF NOT EXISTS "artists" (
                            artist_id VARCHAR, name VARCHAR, location VARCHAR, 
                            latitude NUMERIC, longitude NUMERIC,
                            PRIMARY KEY(artist_id));
                      ''')
                       
    
# Dimension Tables
# The table only stores the most latest information of all users in the app from 
# the staging events table based on his/her latest timestamp. Here user_id acts as the primary key.
# Attributes: user_id, first_name, last_name, gender, level
user_table_create = ('''
                        CREATE TABLE IF NOT EXISTS "users" (
                            user_id INT PRIMARY KEY, first_name VARCHAR, 
                            last_name VARCHAR, gender VARCHAR, level VARCHAR);
                    ''')


# Dimension Tables
# Create songs table which stores songs in music database where song_id and artist_id are the composite primary key.
# Attributes: song_id, title, artist_id, year, duration
song_table_create = ('''
                        CREATE TABLE IF NOT EXISTS "songs" (
                            song_id VARCHAR, title VARCHAR, 
                            artist_id VARCHAR, year INT, duration NUMERIC,
                            primary key(song_id, artist_id));
                    ''')


# Dimension Tables
# Create time table with start_time as the only primary key whose type is timestamp.
# The table only stores timestamps of records in songplays broken down into specific units
# Attributes: start_time, hour, day, week, month, year, weekday
time_table_create = ('''
                        CREATE TABLE IF NOT EXISTS "time" (
                            start_time TIMESTAMP PRIMARY KEY, 
                            hour INT, day INT, week INT, 
                            month INT, year INT, weekday INT);
                    ''')


# STAGING TABLES
staging_events_copy = ''' 
                         COPY staging_events FROM {0}
                         CREDENTIALS 'aws_iam_role={1}'
                         JSON {2} REGION 'us-west-2';
                      '''.format(LOG_DATA, ARN, LOG_JSONPATH)


staging_songs_copy = ''' 
                         COPY staging_songs FROM {0}
                         CREDENTIALS 'aws_iam_role={1}'
                         JSON 'auto' REGION 'us-west-2';
                     '''.format(SONG_DATA, ARN)


# FINAL TABLES
# Fact Table
# Create songplays table which stores records in event data associated with song plays i.e. records with page NextSong
# Attributes: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
songplay_table_insert = ('''
                            INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
                            SELECT start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
                            FROM
                              (SELECT artist artist_name, song title, 
                                   (TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second') start_time, 
                                   userid user_id, level, sessionid session_id, location, useragent user_agent
                              FROM staging_events WHERE page = 'NextSong' AND title IS NOT NULL) songs INNER JOIN
                              (SELECT DISTINCT song_id, title, artist_id FROM staging_songs 
                              WHERE song_id > 0 AND artist_id > 0) songs_info
                              USING (title);
                         ''')


# Dimension Tables
# Create artists table which stores artists in music database.
# Attributes: artist_id, name, location, lattitude, longitude
artist_table_insert = ('''
                        INSERT INTO artists
                        SELECT artist_id, artist_name name, artist_location AS location, 
                                AVG(artist_latitude) AS latitude, AVG(artist_longitude) AS longitude
                        FROM staging_songs WHERE artist_id IS NOT NULL AND artist_name IS NOT NULL
                        GROUP BY 1, 2, 3;
                       ''')


# Dimension Tables
# The table only stores the most latest information of all users in the app from 
# the staging events table based on his/her latest timestamp. Here user_id acts as the primary key.
# Attributes: user_id, first_name, last_name, gender, level
user_table_insert = ('''
                        INSERT INTO users
                        SELECT userid user_id, firstname first_name, 
                                               lastname last_name, 
                                               gender, level
                        FROM
                            (SELECT userid, MAX(ts) AS ts
                            FROM staging_events WHERE userid > 0 GROUP BY 1) A 
                            INNER JOIN (SELECT * FROM staging_events) B USING (userid, ts);
                     ''')


# Dimension Tables
# Create songs table which stores songs in music database where song_id and artist_id are the composite primary key.
# Attributes: song_id, title, artist_id, year, duration
song_table_insert = ('''
                        INSERT INTO songs
                        SELECT song_id, MAX(title) title, artist_id,
                                CAST(AVG(year) AS INT) AS year, AVG(duration) AS duration
                        FROM staging_songs WHERE song_id > 0 AND artist_id > 0 GROUP BY 1, 3;
                     ''')


# Dimension Tables
# Create time table with start_time as the only primary key whose type is timestamp.
# The table only stores timestamps of records in songplays broken down into specific units
# Attributes: start_time, hour, day, week, month, year, weekday
time_table_insert = ('''
                        INSERT INTO time
                        SELECT *, CAST(date_part(hr, start_time) AS INT) AS hour, 
                                  CAST(date_part(d, start_time) AS INT) AS day, 
                                  CAST(date_part(w, start_time) AS INT) AS week,
                                  CAST(date_part(mon, start_time) AS INT) AS month,
                                  CAST(date_part(yr, start_time) AS INT) AS year,
                                  CAST(date_part(weekday, start_time) AS INT) AS weekday          
                        FROM
                            (SELECT DISTINCT (TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second') start_time
                            FROM staging_events WHERE ts > 0);
                     ''')


# QUERY LISTS
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, 
                        user_table_create, song_table_create, artist_table_create, time_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, 
                      user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

