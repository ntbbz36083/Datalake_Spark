To read one folder in song_data, which is 10 files, it takes 390s. 

Project summary
1. Introduction

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

In this project, we will build an ETL pipeline that extracts data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow our analytics team to continue finding insights in what songs the users are listening to.

2. File Description 
In this repository, you will see Test.ipynb, etl_with_local_data.py, dwh.cfg, etl.py, README.md, data folder and Output folder.
Test.ipynb: this provides a script that you can use to run this project.
etl_with_local_data.py: this is the script that will load data from local via spark and save them back to local.
dwh.cfg: a basic config file, includes all the basic configuration.
etl.py: this is the script that will load data from s3 via spark and save them back to s3.
README.md: a description of this project.
data: this is where the local data stays
Output: this is the place that data will save to.

3. Project Description
The data lake we created for this project has 5 tables: 1 fact table and 4 dimension tables
Fact table: 
songplays - records in event data associated with song plays i.e. records with page NextSong, columns have songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
Dimension tables: 
users - users in the app, columns have userId, firstName, lastName, gender, level
songs - songs in music database, columns have song_id, title, artist_id, year, duration
artists - artists in music database, columns have artist_id, name, location, lattitude, longitude
time - columns have timestamps of records in songplays broken down into specific units, start_time, hour, day, week, month, year, weekday

4. ETL Pipeline 
For the ETL pipeline, there are 3 phrases:
A.Loading data from s3 into our data lake.
B.Creating tables with spark.
C.Saving parquet file back to s3. 

To start review this project, please open and run Test.ipynb step by step.

1. First of all, we will load all libaries we need and create a spark session with create_spark_session() function. Then, we load log file and song file into our session. 
2. With spark, we create dataframes with select statements and apply some data manipulation with data.
3. After all above, we save the data to s3 and complete our job. 
If you want to display the file you saved on s3, you can run display_file_s3() function.
If you want to look at the data you saved locally, you can go to Output folder and find them there.
You can run Delete_All() function to delete all files you created locally.
 
Here are some tips for this project:
loading time
For etl_with_local_data.py, in process_song_data() function, I only loaded data/song_data/A/A/A/*.json. Also in process_log_data(), when we need to join with song tabes, I only used  data/song_data/A/A/A/*.json too. And this will take 30 seconds. If you want to load all data, you can uncomment the command in the function and you will be able to load all data.

For etl.py, in process_song_data() function, I only loaded data/song_data/A/A/A/*.json. Also in process_log_data(), when we need to join with song tabes, I only used  data/song_data/A/A/A/*.json too. And this will take 150 seconds. If you want to load all data, you can uncomment the command in the function and you will be able to load all data.

I tried to load all songs data into the data lake, however, it is too long for s3 since it contains 17000+ files, for local it is not that long, since song data only has 72 files.  

One issue:
In the last step of process_log_file function, in the comment we have "write songplays table to parquet files partitioned by year and month", however, we can't do this since this table doesn't have these 2 columns, not sure whether this is a typo.