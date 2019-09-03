import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl, StringType as Str, IntegerType as Int, DateType as Date, TimestampType as TimeStamp
from pyspark.sql import types as T



config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    This function creates spark session and returns it.
    
    Parameters: 
        None
        
    Returns:
        spark session object
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    This function reads off the song json files from S3 and convert the data into dimension tables for song and artist dimension
    
    Parameters: 
        spark - spark session object 
        input_data - the root path for input data in S3
        output_data - the root path for output data
        
    Returns:
        None
    """
    # get filepath to song data file
    song_data = "{}/song_data/*/*/*/*".format(input_data)
     
    # define schema used to import song json data into our data frame
    songSchema = R([
            Fld("num_songs", Int()),
            Fld("artist_id", Str()),
            Fld("artist_latitude", Dbl()),
            Fld("artist_longitude", Dbl()),
            Fld("artist_location", Str()),
            Fld("artist_name", Str()),
            Fld("song_id", Str()),
            Fld("title", Str()),
            Fld("duration", Dbl()),
            Fld("year", Int())

        ])
    
    # read song data file
    df = spark.read.format("json").load(song_data, schema=songSchema)

    # register the temp view from our dataframe df_song
    df.createOrReplaceTempView("df_song")
    
    # extract columns to create songs table
    songs_table = spark.sql("""
                         SELECT DISTINCT song_id, 
                                        title,
                                        artist_id, 
                                        artist_name,
                                        year, 
                                        duration FROM df_song
                            """)
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').parquet('{}/dim_song.pq'.format(output_data))

    # extract columns to create artists table
    artists_table = spark.sql("""
                         SELECT DISTINCT artist_id,
                                        artist_name,
                                        artist_location,
                                        artist_latitude,
                                        artist_longitude
                                FROM df_song
                        """)
    
    # write artists table to parquet files
    artists_table.write.parquet('{}/dim_artist.pq'.format(output_data))


def process_log_data(spark, input_data, output_data):
    """
    This function reads off the song json files from S3 and convert the data into dimension tables for song and artist dimension
    
    Parameters: 
        spark - spark session object 
        input_data - the root path for input data in S3
        output_data - the root path for output data
        
    Returns:
        None
    """
    # get filepath to log data file
    log_data = "{}/log_data/*/*/*".format(input_data)

    # read log data file
    df = spark.read.format("json").load(log_data)
    
    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp( (x/1000.0) ), T.TimestampType()) 
    df = df.withColumn("listening_datetime", get_timestamp(df.ts))
    
    # Using the same udf to convert epoch ts into timestamp already. 
    #get_datetime = udf()
    #df = 
    
    # filter by actions for song plays
    df = df.where("page == 'NextSong'")
    
    # register dataframe df_log into the temp view
    df.createOrReplaceTempView("df_log")
    # extract columns for users table    
    users_table = spark.sql("""
                  select DISTINCT userId,
                                firstName,
                                lastName,
                                gender,
                                level
                        FROM df_log as e
                """)

    
    # write users table to parquet files
    users_table.write.parquet('{}/dim_user.pq'.format(output_data))

   
   
    
    # extract columns to create time table
    time_table = spark.sql("""
                         SELECT DISTINCT listening_datetime as t_start_time,
                             hour(listening_datetime) as t_hourofday,
                             day(listening_datetime) as t_daynuminmonth,
                            weekofyear(listening_datetime) as t_weeknuminyear,
                             month(listening_datetime) as t_monthnuminyear,
                             year(listening_datetime) as t_yearnuminyear,
                             dayofweek(listening_datetime) as t_daynuminweek


                                    FROM df_log as s
                        """)
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('t_yearnuminyear', 't_monthnuminyear').parquet('{}/dim_time.pq'.format(output_data))

    # read in song data to use for songplays table
    song_df = spark.read.parquet('data/analytics/dim_song.pq')
    
    # registering temp view for dim_time and df_song in order to join in the songplay 
    song_df.createOrReplaceTempView("df_song")
    time_table.createOrReplaceTempView("dim_time")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
                        SELECT DISTINCT e.listening_datetime   AS t_start_time,
                                        userId as u_user_id,
                                        level as u_level,
                                        song_id as s_song_id,
                                        artist_id as a_artist_id,
                                        sessionId as sp_session_id,
                                        location as sp_location,
                                        userAgent as sp_user_agent,
                                        t.t_yearnuminyear,
                                        t.t_monthnuminyear
                                    FROM  df_log as e
                                        JOIN df_song s
                                            ON (e.artist = s.artist_name)
                                        JOIN dim_time t
                                            on t.t_start_time = e.listening_datetime
                              
                        """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('t_yearnuminyear', 't_monthnuminyear').parquet('{}/songplays.pq'.format(output_data))


def main():
    """
    This function is the main process of the script
    
    Parameters: 
        None
        
    Returns:
        None
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "data/analytics"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
