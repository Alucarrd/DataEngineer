music_history_sessionId_itemInSession_table_drop = "DROP TABLE IF EXISTS music_history_by_sessionId_itemInSession"
music_history_userId_sessionId_table_drop = "DROP TABLE IF EXISTS music_history_by_userId_session_d"
music_history_song_table_drop = "DROP TABLE IF EXISTS music_history_by_song"


music_history_sessionId_itemInSession_table_create = """
CREATE TABLE IF NOT EXISTS music_history_by_sessionId_itemInSession (sessionId int, itemInSession int, artist text, song text, song_length decimal, PRIMARY KEY (sessionId, itemInSession))

"""

music_history_userId_sessionId_table_create = """
CREATE TABLE IF NOT EXISTS music_history_by_userId_session_d (userId int, sessionId int, itemInSession int, artist text, song text, firstName text, lastName text,  PRIMARY KEY ((userId, sessionId), itemInSession))

"""

music_history_song_table_create = """
CREATE TABLE IF NOT EXISTS music_history_by_song (song text, userId int, firstName text, lastName text, PRIMARY KEY (song, userId))

"""

insert_music_history_sessionId_itemInSession = """
INSERT INTO music_history_by_sessionId_itemInSession(sessionId, itemInSession, artist, song, song_length) VALUES (%s, %s, %s, %s, %s)
"""
insert_music_history_userId_sessionId = """
INSERT INTO music_history_by_userId_session_d(userId, sessionId, itemInSession, artist, song, firstName, lastName) VALUES (%s, %s, %s, %s, %s, %s, %s)
"""
insert_music_history_song = """
INSERT INTO music_history_by_song(song, userId, firstName, lastName) VALUES (%s, %s, %s, %s)
"""

select_by_sessionId_itemInSession = """
    SELECT artist, song, song_length FROM music_history_by_sessionId_itemInSession WHERE sessionId=%s AND itemInSession=%s
"""
select_by_userId_sessionId = """
    SELECT artist, song, firstName, lastName FROM music_history_by_userId_session_d WHERE userId=%s AND sessionId=%s
"""

select_by_song = """
SELECT firstName, lastName FROM music_history_by_song where song='%s'
"""

create_table_queries = [music_history_sessionId_itemInSession_table_create,music_history_userId_sessionId_table_create, music_history_song_table_create ]
drop_table_queries = [music_history_sessionId_itemInSession_table_drop,music_history_userId_sessionId_table_drop , music_history_song_table_drop]