# Project Data Warehouse of the Nanodegree Data Engineering

### The repository contains the following files:
- `create_tables.py`
    - the script first drops existing tables if they exist and then creates new tables
- `sql_queries.py`
    - the script contains the sql queries to drop, create, stage and insert the data into the tables
- `etl.py`
    - the script executes the staging of the S3 data to redshift and inserts the data into the respective tables of the data schema
- `dwh.cfg`
    - this file contains all necessary config information
- `redshift_sparkify.ipynb`
    - this notebook sets up a Redshift cluster and its respective IAM Role using IaC to do so. 

### Purpose of the project

The purpose of the project is to provide sparkify with an easily queriable database which allows for convenient data analytics and an efficient way to store its data.

### Database Schema

The database schema consists of the following tables:

- staging_events: staging table for events data
- staging_songs: staging table for songs data
- songplays: a fact table, containing information about what song was played when and by which user
- users: dim table, containing information about the sparkify user
- songs: dim table, containing information about the songs like title, length
- artists: dim table, containing information about the artists like name and location
- time: dim table, extracting information from the timestamp when a song was played like day and month

### ETL Pipeline
The ETL Pipeline 
1. loads the data from different S3 buckets
2. then stages the data into the tables staging_events and staging_songs
3. and lastly divides it into their respective tables in the database schema stated above.

### Example Queries

`SELECT sp.start_time, u.first_name, s.title
FROM songplay AS sp 
JOIN users AS u ON sp.user_id = u.user_id 
JOIN songs AS s ON sp.song_id = s.song_id
WHERE sp.song_id = 'SOVAEBW12AB0182CE6'`