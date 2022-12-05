import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.amazonaws:aws-java-sdk-pom:1.10.34,org.apache.hadoop:hadoop-aws:2.7.2 pyspark-shell'


def create_spark_session():
    ''' Creates spark session '''
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    
    '''
      Description: processes song datafiles and extracts data and saves it in songs and artists tables/folders)
      Parameters:
         spark: spark session handle
         input_data: input data path to read all songs data
         otput_data: output path to write tables to folders
    '''
    
    # get filepath to song data file
    
    song_data = input_data+"song_data/*/*/*/*.json"
    
    # read song data file
    df = spark.read.json(song_data,mode='PERMISSIVE')

    # extract columns to create songs table
    songs_table = df['song_id', 'title', 'artist_id', 'year', 'duration'].drop_duplicates()
   
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(output_data+"songs/", mode="overwrite",partitionBy=['year','artist_id'])
 
    # extract columns to create artists table
    artists_table = df.selectExpr('artist_id', 'artist_name as name', 'artist_location as location', 'artist_latitude as latitude', 'artist_longitude as longitude').drop_duplicates()
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data+"artists/", mode="overwrite")


def process_log_data(spark, input_data, output_data):
    
    '''
      Description: processes log datafiles and extracts data and saves it in users and time tables/folders.
                   It also gathers data from dimenion tables ad log files and creates songsplay fact table/folder.
      Parameters:
         spark: spark session handle
         input_data: input data path to read all logs data
         otput_data: output path to write tables to folders
    '''
    
    # get filepath to log data file
    log_data = input_data+"/log_data/*.json"

    # read log data file
    df = spark.read.json(log_data)
  
    # filter by actions for song plays
    df = df.filter(df['page'] == "NextSong")
  
    # extract columns for users table    
    users_table = df.selectExpr("userId as user_id", "firstName as first_name", "lastName as last_name", "gender", "level").drop_duplicates()
    
    # write users table to parquet files
    users_table.write.parquet(output_data+"users/", mode="overwrite")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x:datetime.utcfromtimestamp(int(x)/1000), TimestampType())
    df=  df.withColumn("start_time",get_timestamp(df.ts))
    
    
    
    # extract columns to create time table
    time_table = df.withColumn("year",year(df.start_time)).\
                            withColumn("month",month(df.start_time)).\
                            withColumn("week",month(df.start_time)).\
                            withColumn("day",month(df.start_time)).\
                            withColumn("hour",month(df.start_time)).\
                            withColumn("weekday",month(df.start_time)).\
                            select("ts","start_time","hour", "day", "week", "month", "year", "weekday").drop_duplicates()
    
 
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(output_data+"time/",mode='overwrite',partitionBy=['year','month'])
   
    # read in song data to use for songplays table
    song_df = spark.read.format('parquet').load(os.path.join(output_data,'songs/'))

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df,df['song']==song_df['title'],how='inner').\
                         select(monotonically_increasing_id().alias('songplay_id'),\
                                'start_time',col('userId').alias('user_id'),'level', \
                                'song_id', 'artist_id', col('sessionId').alias('session_id'),\
                                'location', col('userAgent').alias('user_agent'))
    songplays_table=songplays_table.join(time_table,df['start_time']==songplays_table['start_time']).\
                   select('songplay_id',df['start_time'],'user_id','level','song_id', 'artist_id',\
                         'session_id','location','user_agent','year','month')

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(output_data+"/songplays",mode='overwrite',partitionBy=['year','month'])
    


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data= "s3a://udacity-dlake-output/"
    #input_data="data/input_data/"
    #output_data = "data/output_data/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
