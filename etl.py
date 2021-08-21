import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, from_unixtime


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Creates the Spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Processes song data to read from S3 & persist to HDFS
    
    spark : the spark context
    input_data : path to the location of the input data files
    output_data : path to the location where output should be stored
    """

    # get filepath to song data file
    song_data = f'{input_data}song_data'

    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration').drop_duplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(f'{output_data}songs', partitionBy=['year', 'artist_id'])

    # extract columns to create artists table
    artists_table = (df.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude')
                     .drop_duplicates()
                     .withColumnRenamed('artist_name', 'name')
                     .withColumnRenamed('artist_location', 'location')
                     .withColumnRenamed('artist_latitude', 'latitude')
                     .withColumnRenamed('artist_longitude', 'longitude')
                    )
    
    # write artists table to parquet files
    artists_table.write.parquet(f'{output_data}artists')


def process_log_data(spark, input_data, output_data):
    """
    Processes log data to read from S3 & persist to HDFS
    
    spark : the spark context
    input_data : path to the location of the input data files
    output_data : path to the location where output should be stored
    """
    # get filepath to log data file
    log_data = f'{input_data}log_data'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table
    users_table = (df.select('userId', 'firstName', 'lastName', 'gender', 'level')
                   .drop_duplicates()
                   .withColumnRenamed('userId', 'user_id')
                   .withColumnRenamed('firstName', 'first_name')
                   .withColumnRenamed('lastName', 'last_name')
                  )
    
    # write users table to parquet files
    users_table.write.parquet(f'{output_data}users')

    # create timestamp column from original timestamp column
    # get_timestamp = udf()
    # df = 
    
    # create datetime column from original timestamp column
    # get_datetime = udf()
    df = df.withColumn('start_time', from_unixtime(df['ts']/1000))
    
    # extract columns to create time table
    time_table = df.select('start_time', hour(df['start_time']).alias('hour'), dayofmonth(df['start_time']).alias('day'),
                           weekofyear(df['start_time']).alias('week'), month(df['start_time']).alias('month'),
                           year(df['start_time']).alias('year'))
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(f'{output_data}time', partitionBy=['year', 'month'])

    # read in song data to use for songplays table
    song_df = spark.read.json(f'{input_data}song_data')

    # extract columns from joined song and log datasets to create songplays table
    cond = [df.song==song_df.title, df.artist==song_df.artist_name, df.length==song_df.duration]
    songplays_table = (df.join(song_df, on=cond)
                       .select('start_time', col('userId').alias('user_id'), 'level', 'song_id', 'artist_id',
                               col('sessionId').alias('session_id'), 'location', col('userAgent').alias('user_agent'))
                      )

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(f'{output_data}songplays')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "hdfs:///user/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
