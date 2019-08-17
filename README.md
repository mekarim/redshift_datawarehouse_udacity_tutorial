# Project: Data Warehouse


### Introduction [Adapted from Udacity Project Page]
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

In this project, an ETL pipeline was built which extracts the data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for the analytics team to continue finding insights in what songs their users are listening to.


### Running the code (with basic script explanation)

Open the terminal and run the following:

```python create_tables.py```

Drops and create tables based on sql_queries.py. sql_queries.py defines 5 tables and specifies all columns for each of these tables with the right data types and conditions.

```python etl.py```

Extracts and loads log_data and song_data into staging tables in Redshift, from the S3. Performs ETL process on the staging tables to insert data correctly into the 5 tables while handling duplicate records where appropriate.

### Authors
* **[Mohammad Ehsanul Karim](https://github.com/mekarim)**


Reference/Note: The readme.md file was adapted from [PurpleBooth](https://gist.github.com/PurpleBooth/109311bb0361f32d87a2).