This project is an ETL package that is designed to help a music streaming company startup Sparky to transform the user activity logs into a postgresql relational databse. There are two types of files we are trying to import: songs meta data in json and user activity logs in csv.  The ETL package will first extract the songs and artists metadata from the json file, and insert them into the songs and artists table.  Once the songs' data are populated, it will then look into extracting useful information from the activity logs, including users, time, and songplay.

This project consists of several files:

sql_queries.py
    This python file includes the sql statements to drop and create table.  It also holds the template string for insert statements.
    
create_tables.py
    This python file allows you to refresh the entire database by dropping all the table and recreate them. 
    
etl.py
    This python file holds the logic on this etl process.  It will read and parse all the log files inside the data folder, then insert them into database. It is designed to help the sparkify music streaming start to import the user activity logs for futher analysis.
    
etl.ipynb
    This is a jupyter notebook file for us to develop the etl process.

test.ipynb
    This is just a jupyter notebook file for us to explore the content stored in the database.


With the information we have in songplay, we will be able to pass the data into data science team for further analysis.  We could use it to power recommendation system using algorithm such as collaborate filtering.  The below is a sample of what we can do with it:

The below query will return all the songs that's been played more than 5 times by different users

SELECT a.song_id, count(1) as play_count
    from songplay a 
        INNER JOIN songplay b
            ON a.song_id = b.song_id
                WHERE a.user_id != b.user_id
        GROUP BY a.song_id 
            HAVING count(1) > 5
            ORDER BY count(1) DESC
            
Once we have the result set, we can then pick out the to common songs that then we can query out couple users that listens to the top common songs and recommend the songs where user a listened that user b hasn't to user b.
