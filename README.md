# Modeling & transforming Sparkify data using Spark

## Project Summary
This project models & transforms the song & user log data for the Sparkify company using Spark so that it can be analyzed by their analytical team. It uses a star-schema with Dimension & Fact tables to make it easy to write analytical SQL queries. It uses a Python script (which uses Pyspark) to ETL the data from the song & log files in S3 to parquet files on HDFS.

## How to run the project
Please run the following Python scripts :
1. etl.py --> It loads the data into Spark data-frames & then, saves them to HDFS

## Explanation of files
You'll find the following files in the repo :
1. etl.py reads and processes song_data and log_data S3 files and writes them to HDFS.
1. README.md provides discussion on your project.
1. dl.cfg provides config values for the scripts
