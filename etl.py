from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import desc
from pyspark.sql.functions import monotonically_increasing_id

def create_spark_session():
    '''
    Creates and returns a SparkSession
    '''
    spark = SparkSession \
        .builder \
        .appName("DataLakeJob") \
        .getOrCreate()
    return spark

def extract_songs_table(df):
    '''
    Helper function to extract the songs dimension table from songs data

    Arguments:
        df -- songs dataframe

    Returns a Spark Dataframe
    '''
    song_cols = ['song_id', 'title', 'artist_id', 'year', 'duration']
    return df.select(song_cols).dropDuplicates()

def extract_artists_table(df):
    '''
    Helper function to extract the artists dimension table from songs data

    Arguments:
        df -- songs dataframe

    Returns a Spark Dataframe
    '''
    artist_cols = ['artist_id', 'artist_name', 'artist_location',
                   'artist_latitude', 'artist_longitude']
    return df.select(artist_cols).dropDuplicates()

def extract_users_table(df):
    '''
    Helper function to extract the users dimension table from logs data

    Arguments:
        df -- logs dataframe

    Returns a Spark Dataframe
    '''
    user_cols = ['userId', 'firstName', 'lastName', 'gender', 'level', 'ts']

    #Sorted by timestamp to get the latest entry in the dataset
    #when dropping duplicates
    users_table = df.select(user_cols) \
                    .sort(desc('ts')) \
                    .dropDuplicates(['userId']) \
                    .select(user_cols[:-1])
    return users_table

def extract_time_table(df):
    '''
    Helper function to extract the time dimension table from logs data

    Arguments:
        df -- logs dataframe

    Return a Spark Dataframe
    '''
    time_table = df.select(
                        'start_time',
                        hour('start_time').alias('hour'),
                        dayofmonth('start_time').alias('day'),
                        weekofyear('start_time').alias('week'),
                        month('start_time').alias('month'),
                        year('start_time').alias('year'),
                        date_format('start_time', 'u').alias('weekday')
                ).dropDuplicates()
    return time_table

def fetch_song_data_with_artist_name(spark, output_data, song_data_location, artist_data_location):
    '''
    Helper function to fetch the recently created song dim table and append artist name to it.
    It is used to be joined with the songplay fact table so that it includes song_id and artist_id.
    The dataframe is then broadcasted to the worker nodes for a more efficient join.

    Arguments:
        spark -- SparkSession
        output_data -- S3 Bucket where the file is saved
        song_data_location -- song dimension table location within S3 Bucket
        artist_data_location -- artist dimension table location within S3 Bucket

    Return a Spark Dataframe
    '''
    song_path = os.path.join(output_data, song_data_location)
    artist_path = os.path.join(output_data, artist_data_location)
    artist_cols = ['artist_id', 'artist_name']
    song_cols = ['song_id', 'title', 'artist_id', 'artist_name', 'duration']
    artist_df = spark.read.parquet(artist_path).select(artist_cols).withColumnRenamed('artist_id', 'join_id')
    song_df = spark.read.parquet(song_path)

    song_df_bc = broadcast(
                    song_df.join(artist_df, artist_df.join_id == song_df.artist_id) \
                           .select(song_cols)
                        )
    return song_df_bc

def extract_songplay_data(df, song_df_bc):
    '''
    Helper function to extract the songplay fact table from logs data

    Arguments:
        df -- logs Dataframe
        song_df_bc -- song dimension table

    Return a Spark Dataframe
    '''
    songplays_cols = ['id', 'start_time', 'userId', 'level', 'song_id', 'artist_id', 'sessionId', 'location', 'userAgent',
                      year('start_time').alias('year'), month('start_time').alias('month')]
    songplays_table = df.join(song_df_bc, (df.song == song_df_bc.title) & (df.artist == song_df_bc.artist_name) & (df.length == song_df_bc.duration), how='left') \
                        .withColumn("id", monotonically_increasing_id()) \
                        .select(songplays_cols)
    return songplays_table

def write_parquet_file_with_partition(df, output_data, file_location, partition_cols):
    '''
    Helper function to write a parquet file that partitions the data

    Arguments:
        df -- Spark Dataframe
        output_data -- S3 Bucket where the file is saved
        file_location -- file location within the S3 Bucket
        parition_cols -- columns with which the data will be partitioned
    '''
    output_path = os.path.join(output_data, file_location)
    df.write.mode('overwrite').partitionBy(partition_cols).parquet(output_path)

def write_parquet_file_no_partition(df, output_data, file_location):
    '''
    Helper function to write a parquet file WITHOUT partitioning the data

    Arguments:
        df -- Spark Dataframe
        output_data -- S3 Bucket where the file is saved
        file_location -- file location within the S3 Bucket
    '''
    output_path = os.path.join(output_data, file_location)
    df.write.mode('overwrite').parquet(output_path)

def process_song_data(spark, input_data, output_data):
    '''
    - Reads songs data from S3 Bucket
    - Processes songs dimension table
    - Processes artists dimension table

    Arguments:
        spark - SparkSession
        input_data -- S3 Bucket where the data lives
        output_data -- S3 Bucket where the file will be written
    '''
    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')

    # read song data file
    df = spark.read.json(song_data)

    # process songs table
    song_data = extract_songs_table(df)
    partition_cols = ['year', 'artist_id']
    write_parquet_file_with_partition(song_data, output_data, 'song_table/song_data.parquet', partition_cols)

    # process artists table
    artist_data = extract_artists_table(df)
    write_parquet_file_no_partition(artist_data, output_data, 'artist_table/artist_data.parquet')

def process_log_data(spark, input_data, output_data):
    '''
    - Reads logs data from S3 Bucket
    - Processes users dimension table
    - Processes time dimension table
    - Processes songplay fact table

    Arguments:
        spark - SparkSession
        input_data -- S3 Bucket where the data lives
        output_data -- S3 Bucket where the file will be written
    '''
    # get filepath to log data file
    log_data = os.path.join(input_data, 'log_data')

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # process users table
    users_table = extract_users_table(df)
    write_parquet_file_no_partition(users_table, output_data, 'user_table/users_data.parquet')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda ts : datetime.fromtimestamp(ts / 1000), TimestampType())
    df = df.withColumn('start_time', get_timestamp(df.ts))

    # process time table
    time_table = extract_time_table(df)
    partition_cols = ['year', 'month']
    write_parquet_file_with_partition(time_table, output_data, 'time_table/time_data.parquet', partition_cols)

    # process songplays data
    song_df_bc = fetch_song_data_with_artist_name(spark, output_data, 'song_table/song_data.parquet', 'artist_table/artist_data.parquet')
    songplays_table = extract_songplay_data(df, song_df_bc)
    partition_cols = ['year', 'month']
    write_parquet_file_with_partition(songplays_table, output_data, 'songplay_table/songplay_data.parquet', partition_cols)

def main():
    spark = create_spark_session()
    input_data = 's3n://udacity-dend'
    output_data = 's3n://my-udacity-projects/'

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)

    spark.stop()

if __name__ == "__main__":
    main()
