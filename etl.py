#Import libraries
import configparser
import os
from datetime import datetime
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, BooleanType, DateType, FloatType
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofweek, dayofmonth, hour, weekofyear, date_format, isnan, count, when, col, desc, udf, sort_array, asc, avg, rank, mean, first, ceil, rand, last, monotonically_increasing_id, row_number

#Read config file
config = configparser.ConfigParser()
config.read('dl.cfg')

#Write AWS aceess and secret access key into enviroment variables
os.environ['AWS_ACCESS_KEY_ID']=config['AWS_KEYS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_KEYS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    This functiob creates sparks session
    
    Input: No specific input
    Output: Spark Session
    """
    
    #Get or create spark session
    print('Creating Spark Session')
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    
    #Modify spark broadcastTimeout
    spark.conf.set("spark.sql.broadcastTimeout", 30000)
    
    #Modify Spark context mapreduce algorithm
    sc = spark.sparkContext
    sc._jsc.hadoopConfiguration().set("mapreduce.fileoutputcommitter.algorithm.version", "2")
    print('Spark Session created successfully')
    
    return spark


def process_song_data(spark, input_data, output_data):
    """
    This function processes song data from song data located in AWS S3 and writes back to S3 in parquet form. It also returns songs table for later use
    
    Input: Spark session, input data path and output data path
    Output: Songs table created
    """
    
    # get filepath to song data file
    song_data = os.path.join(input_data,'song_data/A/A/A/*.json')
    
    # read song data file
    print('Reading song_data from S3')
    df = spark.read.json(song_data)
    
    print('Schema of song data: ')
    df.printSchema()
    
    # extract columns to create songs table
    songs_table = df.select(['song_id', 'title', 'artist_id', 'year', 'duration']).distinct() 
    
    # write songs table to parquet files
    print('Writing songs_table')
    songs_table.write.mode('overwrite').parquet(output_data + 'songs.parquet', partitionBy = ['song_id'])

    # extract columns to create artists table
    artists_table = df.select(['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']).distinct()
    
    # write artists table to parquet files
    print('Writing artists_table')
    artists_table.write.mode('overwrite').parquet(output_data + 'artists.parquet', partitionBy = ['artist_id'])
    
    return songs_table

def process_log_data(spark, input_data, output_data, songs_table):
    """
    This function processes log data from song data located in AWS S3 and writes back to S3 in parquet form. 
    
    Input: Spark session, input data path, output data path and songs table
    Output: No specific output
    """
    
    #Get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'    
    
    #Read log data file
    print('Start reading log data')
    df = spark.read.json(log_data)
    print('Finish reading log data')
    
    #Print schema of log data
    print('Schema of log data')
    df.printSchema()
    
    #Drop duplicates and then sort by timestamp, finally filter actions for song plays
    df = df.dropDuplicates()
    df = df.sort("ts", ascending = True)
    df = df.filter(df['page'] == 'NextSong')

    #Extract columns for users table    
    users_table = df.select(['userId','firstName','lastName','gender','level']).distinct()
    users_table = users_table.groupBy(['userId','firstName','lastName','gender']).agg(last('level').alias('level'))
    
    # Write users table to parquet files
    print('Writing users_table')
    users_table.write.mode('overwrite').parquet(output_data + 'users.parquet', partitionBy = ['userId'])

    #Create timestamp(in s) column from original ts(in ms)column
    get_timestamp = udf(lambda x : float(x)/1000, FloatType())
    df = df.withColumn('timestamp', get_timestamp('ts'))
    
    #Create start_time column from timestamp column
    df = df.withColumn('start_time', df.timestamp.cast('timestamp'))
    
    #Extract columns to create time table
    time_table = df.select(['start_time', hour('start_time').alias('hour'), dayofmonth('start_time').alias('day'), weekofyear('start_time').alias('week'), month('start_time').alias('month'), year('start_time').alias('year'), dayofweek('start_time').alias('weekday')]).dropDuplicates(['start_time'])
 
    #Write time table to parquet files 
    print('Writing time_table')
    time_table.write.mode('overwrite').parquet(output_data + 'time.parquet', partitionBy = ['start_time'])

    #Create serial numbers for songplays and join with songs_table
    df = df.withColumn("songplay_id", row_number().over(Window.partitionBy('timestamp').orderBy('timestamp')))
    df = df.join(songs_table,df.song == songs_table.title, how = 'inner')

    #Extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.select(['songplay_id', 'start_time', 'userId', 'level', 'song_id', 'artist_id', 'sessionId', 'location', 'userAgent'])

    #Write songplays table to parquet files
    print('Writing songplays_table')
    songplays_table.write.mode('overwrite').parquet(output_data + 'songplays.parquet', partitionBy = ['songplay_id'])


def main():
    """
    This is main function of this script
    """
    
    #Get or create Spark session
    spark = create_spark_session()
    
    #Define input and output path
    input_data = "s3a://udacity-dend/"   
    output_data = "s3a://burger-bucket/"
    
    #Process song data and log data
    songs_table = process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data, songs_table)


if __name__ == "__main__":
    main()
