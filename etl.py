import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, to_timestamp, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()

config.read_file(open('aws/credentials.cfg'))

os.environ["AWS_ACCESS_KEY_ID"]= config['AWS']['AWS_ACCESS_KEY_ID']
os.environ["AWS_SECRET_ACCESS_KEY"]= config['AWS']['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    ''' Function to read song data from s3 bucket,performs transformations and loads to target tables '''
    
    # get filepath to song data file
    song_data = os.path.join(input_data, "song_data/*/*/*/*.json")
    
    # read song data file
    df_song = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df_song.select("song_id","title", "artist_id","year","duration").dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').parquet(os.path.join(output_data, 'songs.parquet'), 'overwrite')

    # extract columns to create artists table
    artists_table =  df_song.select("artist_id",col("artist_name").alias("name"),col("artist_location").alias("location"),                                 col("artist_latitude").alias("latitude"),col("artist_longitude").alias("longitude")).dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, 'artists.parquet'), 'overwrite')


def process_log_data(spark, input_data, output_data):
    ''' Function to read log data from s3 bucket,performs transformations and loads to target tables '''
    
    # get filepath to log data file
    log_data = os.path.join(input_data,"log_data/*/*/*.json")

    # read log data file
    df_log = spark.read.json(log_data)
    
    # filter by actions for song plays and also filter the records where userId is null
    df_log = df_log.filter(col("page")=='NextSong').filter(df_log.userId.isNotNull())

    # extract columns for users table    
    users_table = df_log.select(col("userId").alias("user_id"),col("firstName").alias("first_name"),\
                        col("lastName").alias("last_name"), "gender", "level").dropDuplicates()
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, 'users.parquet'), 'overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(int(int(x) / 1000)))
    df_log = df_log.withColumn("timestamp",get_timestamp(col("ts")))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x) / 1000.0)))
    df_log = df_log.withColumn("datetime", get_datetime(col("ts")))
    
    # extract columns to create time table
    time_table = df_log.select(
         col('datetime').alias('start_time'),
         hour('datetime').alias('hour'),
         dayofmonth('datetime').alias('day'),
         weekofyear('datetime').alias('week'),
         month('datetime').alias('month'),
         year('datetime').alias('year'),
         date_format('datetime', 'F').alias('weekday')
     )  
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year','month').parquet(os.path.join(output_data, 'time.parquet'), 'overwrite')
    
    # read in song data to use for songplays table
    song_data = os.path.join(input_data, "song_data/*/*/*/*.json")
    song_df = spark.read.json(song_data) 

    # extract columns from joined song and log datasets to create songplays table 
    df_songplay = song_df.join(df_log,
                           (song_df.artist_name == df_log.artist) &
                           (song_df.title == df_log.song) &
                           (song_df.duration == df_log.length))
    
    songplays_table = df_songplay.select(
        monotonically_increasing_id().alias('songplay_id'),
        col('datetime').alias('start_time'),
        col('level').alias('level'),
        col('userId').alias('user_id'),
        col('song_id').alias('song_id'),
        col('artist_id').alias('artist_id'),
        col('sessionId').alias('session_id'),
        col('location').alias('location'),
        col('userAgent').alias('user_agent'),
        col('year').alias('year'),
        month('datetime').alias('month')
        ).dropDuplicates()

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data, 'songplays.parquet'), 'overwrite')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://sparkify-dend/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
