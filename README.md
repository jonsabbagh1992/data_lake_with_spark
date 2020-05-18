Sparkify Data Lake
==================

Sparkify (a startup) collects data on songs and user activity on their new music streaming app. We were particularly interested in understanding what songs users are listening to. This task was difficult as the data resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

This ETL pipeline processes the data from the JSON logs and loads them into a Data Lake stored on S3 using Parquet format. 

Database Schema
===============

The following star schema was optimized to run queries on songs. Here are the details

Facts Table
-----------

1. **songplay**: records in log data associated with song plays i.e. records with page **NextSong**
    - songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

Dimension Tables
-----------

2. **users**: users in the app
    - user_id, first_name, last_name, gender, level


3. **songs** : songs in music database
    - song_id, title, artist_id, year, duration
    - This table is partitioned by year and artist_id


4. **artists**: artists in music database
    - artist_id, name, location, latitude, longitude


5. **time**:timestamps of records in songplays broken down into specific units
    - start_time, hour, day, week, month, year, weekday
    - This table is partitioned by year and month
    
How to Run
=====================
You will need to have your own AWS account set up, with a user that has the following permissions:
    
    - AmazonS3FullAccess

Once that is set-up, you will need to set up your Spark environment to run the **etl.py** spark job; I used an EMR cluster. 

You will also need to create an S3 bucket where the data will be outputted.

Enjoy!


Future Enhancements
====================

I would like to add the functionality to programatically create an EMR cluster for the spark application to be run on. Once it is finished, the cluster will be torn down to avoid unecessary costs.
