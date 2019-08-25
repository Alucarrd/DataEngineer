# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES
#songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays(songplay_id SERIAL not null primary key, start_time timestamp not null, user_id int not null, level varchar not null, song_id varchar not null, artist_id varchar not null, session_id int not null, location varchar, user_agent varchar)

""")
#user_id, first_name, last_name, gender, level
user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (user_id int not null primary key, first_name varchar, last_name varchar, gender char, level varchar not null)
""")
#song_id, title, artist_id, year, duration

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (song_id varchar not null primary key, title varchar not null, artist_id varchar not null, year int, duration decimal)
""")
#artist_id, name, location, latitude, longitude
artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (artist_id varchar not null primary key, name varchar not null, location varchar, latitude decimal, longitude decimal)
""")
#start_time, hour, day, week, month, year, weekday
time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (start_time timestamp not null, hour int not null, day int not null, week int not null, month int not null, year int not null, weekday int not null)
""")

# INSERT RECORDS

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (user_id) DO UPDATE SET level=EXCLUDED.level;
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration) VALUES ( %s, %s, %s, %s, %s ) ON CONFLICT (song_id) DO NOTHING;
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (artist_id) DO NOTHING;
""")


time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday) VALUES (%s, %s, %s, %s, %s, %s, %s)
""")

# FIND SONGS

song_select = ("""
SELECT a.artist_id, s.song_id FROM songs s INNER JOIN artists a ON s.artist_id = a.artist_id \
    WHERE (s.title = %s AND a.name= %s AND s.duration = %s )
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]