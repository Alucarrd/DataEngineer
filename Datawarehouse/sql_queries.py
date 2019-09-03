import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
ARN = config.get('IAM_ROLE','ARN')
# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events_table"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs_table"
songplay_table_drop = "DROP TABLE IF EXISTS sp_songplay"
user_table_drop = "DROP TABLE IF EXISTS u_user"
song_table_drop = "DROP TABLE IF EXISTS s_song"
artist_table_drop = "DROP TABLE IF EXISTS a_artist"
time_table_drop = "DROP TABLE IF EXISTS t_time"

# CREATE TABLES



staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS 
                        staging_events_table (
                            artist TEXT NULL,
                            auth TEXT NULL,
                            firstName VARCHAR NULL,
                            gender CHAR NULL,
                            itemInSession INT NOT NULL,
                            lastName VARCHAR NULL,
                            length DECIMAL NULL,
                            level VARCHAR NULL,
                            location TEXT NULL,
                            method VARCHAR NOT NULL,
                            page VARCHAR NOT NULL,
                            registration DECIMAL NULL,
                            sessionId INT NOT NULL,
                            song TEXT NULL,
                            status VARCHAR NOT NULL,
                            ts VARCHAR NOT NULL,
                            userAgent TEXT NULL,
                            userId INT NULL
                        
                        )

""")
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               
staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS 
                            staging_songs_table (
                                artist_id text NOT NULL,
                                artist_latitude DECIMAL NULL, 
                                artist_location TEXT NULL,
                                artist_longitude DECIMAL NULL,
                                artist_name TEXT NULL, 
                                duration DECIMAL NOT NULL, 
                                num_songs INT NOT NULL, 
                                song_id VARCHAR(20) NOT NULL,
                                title TEXT NOT NULL, 
                                year INT NOT NULL
                            )
""")
#songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS sp_songplay(
                                sp_songplay_id INTEGER NOT NULL IDENTITY(0,1),
                                t_start_time TIMESTAMP NOT NULL,
                                u_user_id INTEGER NOT NULL,
                                u_level VARCHAR(50) NULL,
                                s_song_id VARCHAR(50) NOT NULL,
                                a_artist_id VARCHAR(50) NOT NULL,
                                sp_session_id INTEGER NOT NULL,
                                sp_location TEXT NULL,
                                sp_user_agent TEXT NULL
                                

)
""")
#user_id, first_name, last_name, gender, level

user_table_create = ("""CREATE TABLE IF NOT EXISTS u_user(
                            u_user_id INT NOT NULL sortkey,
                            u_first_name VARCHAR(50) NULL,
                            u_last_name VARCHAR(50) NULL,
                            u_gender CHAR NULL,
                            u_level VARCHAR(50) NULL
)
""")
#song_id, title, artist_id, year, duration

song_table_create = ("""CREATE TABLE IF NOT EXISTS s_song(
                            s_song_id VARCHAR(50) NOT NULL sortkey,
                            s_title TEXT NOT NULL,
                            s_artist_id VARCHAR(50) NOT NULL,
                            s_year INT NOT NULL,
                            s_duration DECIMAL NOT NULL
)
""")
#artist_id, name, location, lattitude, longitude
artist_table_create = ("""CREATE TABLE IF NOT EXISTS 
                            a_artist (
                                a_artist_id VARCHAR(50) NOT NULL sortkey,
                                a_artist_name TEXT NOT NULL,
                                a_artist_location TEXT NULL,
                                a_latitude DECIMAL NULL,
                                a_longitude DECIMAL NULL
                           )
""")

#start_time, hour, day, week, month, year, weekday
time_table_create = ("""CREATE TABLE IF NOT EXISTS 
                            t_time (
                                t_start_time timestamp NOT NULL sortkey,
                                t_hourofday INT NOT NULL,
                                t_daynuminmonth INTEGER NOT NULL,
                                t_weeknuminyear INTEGER NOT NULL,
                                t_monthnuminyear INTEGER NOT NULL, 
                                t_yearnuminyear INTEGER NOT NULL, 
                                t_daynuminweek INTEGER NOT NULL
                            
                            )
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events_table from 's3://udacity-dend/log_data/'
    credentials 'aws_iam_role={}'
    json 's3://udacity-dend/log_json_path.json' region 'us-west-2'

""").format(ARN)

staging_songs_copy = ("""
    copy staging_songs_table from 's3://udacity-dend/song_data/'
    credentials 'aws_iam_role={}'
    json 'auto' region 'us-west-2'

""").format(ARN)


# FINAL TABLES


songplay_table_insert = ("""
INSERT INTO sp_songplay(t_start_time,
                        u_user_id,
                        u_level,
                        s_song_id,
                        a_artist_id,
                        sp_session_id,
                        sp_location,
                        sp_user_agent)
SELECT DISTINCT TIMESTAMP 'epoch' + e.ts/1000 * INTERVAL '1 second'   AS t_start_time,
                userId as u_user_id,
                level as u_level,
                song_id as s_song_id,
                artist_id as a_artist_id,
                sessionId as sp_session_id,
                location as sp_location,
                userAgent as sp_user_agent
            FROM  staging_events_table as e
                JOIN staging_songs_table s
                    ON (e.artist = s.artist_name)
            WHERE e.page = 'NextSong'
""")

user_table_insert = ("""
INSERT INTO u_user(u_user_id, u_first_name, u_last_name, u_gender, u_level)
SELECT DISTINCT userId,
                firstName,
                lastName,
                gender,
                level
        FROM staging_events_table as e
            WHERE e.page = 'NextSong' 
""")
song_table_insert = ("""
INSERT INTO s_song(s_song_id,s_title,s_artist_id, s_year, s_duration )
SELECT DISTINCT song_id, 
                title,
                artist_id, 
                year, 
                duration
        FROM staging_songs_table
                
""")

     

artist_table_insert = ("""
INSERT INTO a_artist(a_artist_id,a_artist_name, a_artist_location,  a_latitude, a_longitude)
SELECT DISTINCT artist_id,
                artist_name,
                artist_location,
                artist_latitude,
                artist_longitude
        FROM staging_songs_table

""")

time_table_insert = ("""
INSERT INTO t_time(t_start_time, t_hourofday, t_daynuminmonth, t_weeknuminyear,t_monthnuminyear, t_yearnuminyear, t_daynuminweek )
SELECT DISTINCT TIMESTAMP 'epoch' + s.ts/1000 * INTERVAL '1 second' as t_start_time,
                EXTRACT(hour FROM t_start_time) as t_hourofday,
                EXTRACT(day FROM t_start_time) as t_daynuminmonth,
                EXTRACT(week FROM t_start_time) as t_weeknuminyear,
                EXTRACT(month FROM t_start_time) as t_monthnuminyear,
                EXTRACT(year FROM t_start_time) as t_yearnuminyear,
                EXTRACT(dow FROM t_start_time) as t_daynuminweek
            FROM staging_events_table as s
            WHERE s.page = 'NextSong'
                
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
