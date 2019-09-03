## Purpose
This project is an ETL package that is design to help the music streaming company startup Sparky to transform the user activity logs from Amazon's S3 and transform them into fact and dimension tables before saving them back into S3 for the analytic team.  To run the package, just type the following:

python etl.py 

## Project Contents
This project consists the following files:

1. etl_development.ipynb - This is the python notebook used to develop the project.
2. dl.cfg - This is the configuration file that stores the AWS access Id and Key.
3. etl.py - This is the main ETL package code.

## Schema Design
This project consists the following fact and dimension tables

### User Dimension:
userId -> Id of the user account
firstName -> First Name of the user
lastName -> Last Name of the user
gender -> Gender of the user
level -> subscription level of the user

### Song Dimension:
song_id -> id of the song in the database
title -> title of the song
artist_id -> id of the artist
artist_name -> name of the artist.  This is column was added as denormalized approach to speed up the join process in order to get the fact table.
year -> year of the record

### Artist Dimension:
artist_id -> Id of the artist
artist_name -> name of the artist
artist_location -> location of the artist
artist_latitude -> latitude of artist's location
artist_longitude -> longitude of the artist's location

### Time Dimension:
t_start_time -> listening time of the record
t_hourofday -> hour of the day for this record
t_daynuminmonth -> day number in month
t_weeknuminyear -> week number in year
t_monthnuminyear -> month number in year
t_yearnuminyear -> year number in year
t_daynuminweek -> day number in week

### SongPlays Fact Table:
t_start_time -> listening time of the record
u_user_id -> user id
u_level -> user level
s_song_id -> song id
a_artist_id -> artist id
sp_session_id -> session id of the play
sp_location -> location data
sp_user_agent -> user agent information
t_yearnuminyear -> yea rnumber in year -> used to partition the data
t_monthnuminyear -> month number in year -> used to partition the data
