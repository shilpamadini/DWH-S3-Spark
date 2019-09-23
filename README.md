# DWH-S3-Spark

This folder contains the necessary program files to create Sparkify Parquet files for analytics to read.
## Contents

1. data
    * Folder contains sample data.
2. etl.ipynb
    * Jupyter notebook file used to build the etl process step by step.
3. etl.py
    * reads and processes files for song_data and log_data and
      loads them into the parquet files.
4. aws
    * Folder contains configuartion details to create spark session on demand
5. README.md
6. environment.yaml
    * conda environment file to import the python environment used by the project.


## Installation

1. Use the following command to clone the project repository.

    ```
    git clone https://github.com/shilpamadini/sparkify-s3-spark.git
    ```

2. Create the environment using below command

    ```
    conda env create -f environment.yaml
    ```

3. Activate the conda environment

    ```
    source activate dand_py3
    ```

5. Navigate to the project directory and run the following to create tables

    ```
    python etl.py
    ```


## Functionality

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

This project aims to build an ETL pipeline that extracts their data from S3, process them using spark, and transforms data into a set of dimensional tables for the analytics team to continue finding insights on the data. These tables are loaded as parquet files into s3 bucket for analytics teams to access.

Analytics are interested in knowing what songs the users are listening to and performing ranking , aggregation on the data to determine which song is played the most, what is most popular song, which artist released most popular songs. Analytics may also be interested in looking at the trends over a period of time.

In order to support the required analytics a star schema design is implemented to design the data warehouse. Songplay table is the fact table and song, user,artist and time are dimension tables.

Here is the ER diagram explaining the schema design.

![Screen Shot 2019-09-22 at 9.47.01 PM.png](https://github.com/shilpamadini/githubimages/blob/master/Screen%20Shot%202019-09-22%20at%209.57.46%20PM.png?raw=true)

Songs data is partitioned by year and artist_id. Time data is partitioned by year and month.
Song Play data is partioned by year and month.
