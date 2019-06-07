import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
from pyspark.sql import functions as F
from pyspark.sql import types as T
import zipfile

# read in config
config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    This function is used to create a spark session to work in
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def process_song_data(spark, input_data, output_data):
    """
    This function is used to load data from local file to our data lake and export as parquet file back to our local folder Output.
    """
    # get filepath to song data file
    song_data = input_data + 'song-data.zip'
    
    # unzip the zip file
    zip_ref = zipfile.ZipFile(song_data, 'r')
    zip_ref.extractall(input_data)
    zip_ref.close()

    # read song data file, using song_data/A/A/A/* for performance
    df = spark.read.json(input_data + 'song_data/A/A/A/*')
    # uncomment if you want to load all data
    #df = spark.read.json(song_data + 'song_data/*/*/*/*')

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration')
    
    # convert the data type to proper data type for each column
    fields_1 = {'song_id':'string','title':'string', 'artist_id':'string', 'year':'int', 'duration':'float'}
    exprs_1 = [ "cast ({} as {})".format(key,value) for key, value in fields_1.items()]
    songs_table = songs_table.selectExpr(*exprs_1)
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year','artist_id').parquet(output_data + "songs.parquet")

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude')
    
    # convert the data type to proper data type for each column
    fields_2 = {'artist_id':'string', 'artist_name':'string', 'artist_location':'string', 'artist_latitude':'string', 'artist_longitude':'string'}
    exprs_2 = [ "cast ({} as {})".format(key,value) for key, value in fields_2.items()]
    songs_table = artists_table.selectExpr(*exprs_2)
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + "artists.parquet")
    

def process_log_data(spark, input_data, output_data):
    """
    This function is used to load data from local file to our data lake and export as parquet file back to our local folder Output.
    """
    # get filepath to log data file
    log_data = input_data + 'log-data.zip'
    
    # unzip the zip file
    zip_ref = zipfile.ZipFile(log_data, 'r')
    zip_ref.extractall(input_data+'log_data')
    zip_ref.close()
    
    # read log data file
    # read song data file
    df = spark.read.json(input_data + 'log_data/*')
    # uncomment if you want to load all data
    #df = spark.read.json(song_data + 'song_data/*/*/*/*')

    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df.select('userId','firstName', 'lastName', 'gender','level')
    
    # convert the data type to proper data type for each column
    fields_3 = {'userId':'string', 'firstName':'string', 'lastName':'string', 'gender':'string', 'level':'string'}
    exprs_3 = [ "cast ({} as {})".format(key,value) for key, value in fields_3.items()]
    users_table = users_table.selectExpr(*exprs_3)
    
    # write users table to parquet files
    users_table.write.parquet(output_data + "users.parquet")
    
    # create datetime column from original timestamp column
    get_timestamp = F.udf(lambda x: datetime.fromtimestamp( (x/1000.0) ), T.TimestampType()) 
    df = df.withColumn('start_time',get_timestamp(col('ts')))

    # extract columns to create time table
    time_table = df.select('start_time', hour('start_time').alias('hour'),dayofmonth('start_time').alias('dayofmonth'), \
                           weekofyear('start_time').alias('weekofyear'), month('start_time').alias('month'), year('start_time').alias('year'),\
                           dayofweek('start_time').alias('dayofweek'))
 
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year','month').parquet(output_data + "time.parquet")

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data + 'song_data/A/A/A/*')
    # uncomment if you want to load all data
    #song_df = spark.read.json(input_data + 'song_data/*/*/*/*')
    
    # create 2 staging table to join with each other
    song_df.createOrReplaceTempView("staging_songs")
    df.createOrReplaceTempView("staging_events")
    
    # Run the query to join staging_songs and staging_events to retrive columns we need
    songplays_table = spark.sql("""
    select start_time, userId, level,song_id, artist_id, sessionId, location, userAgent, artist, title, length \
    from staging_events left join staging_songs on staging_events.artist= staging_songs.artist_name and \
    staging_events.song= staging_songs.title and staging_events.length= staging_songs.duration
    """) 

    # write songplays table to parquet files partitioned by year and month(can't do this since this table doesn't have these 2 columns, not sure whether this is a typo)
    songplays_table.write.parquet(output_data + "songplays.parquet")



def main():
    spark = create_spark_session()
    input_data = "data/"
    output_data = "Output/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()