## pysql-beam

### This package is still under development but has been used in few projects in production. This has been tested with dataflow Runner and Direct runner

This package aim to provide Apache_beam io connector for MySQL and Postgres database.


This package provides apache beam io connector for postgres db and mysql db.
This package wil aim to be pure python implementation for both io connector

FYI: This does not uses any jdbc or odbc connector

Requirements:

    1. Python>=2.7 or python>= 3.5
    2. Apache beam >= 2.10
    3. pymysql[rsa]
    4. psycopg2-binary


Installation:
    
    1. pip install git+git@github.com:MediaAgility/pysql-beam.git
    or 
    2. pip installl pysql-beam


Current functionality:

    1. Read from MySQL database by passing either table name or sql query
    2. Read from Postgres database by passing either table name or sql query


Reference Guide:

1. Java IO connector for the same:
    https://github.com/spotify/dbeam

2. How to write io connector for Apache Beam:
    https://beam.apache.org/documentation/io/developing-io-overview/
    
    https://beam.apache.org/documentation/io/developing-io-python/

Usage Guide:
```
from pysql_beam.sql_io.sql import ReadFromSQL

....
ReadFromSQL(host=self.options.host, port=self.options.port,
        username=self.options.username, password=self.options.password,
        databse=self.options.database,
        query=self.options.source_query,
        wrapper=PostgresWrapper,
        batch=100000)

```
Examples:

    For mysql:
    `python cloud_sql_to_file.py --host localhost --port 3306 --database SECRET_DATABASE --username SECRET_USER --password SECRET_PASSWORD --table YOUR_TABLE --output YOUR_OUTPUT_FLLE`

    For postgres:
    `python cloud_sql_to_file.py --host localhost --port 5432 --database SECRET_DATABASE --username SECRET_USER --password SECRET_PASSWORD --table YOUR_TABLE --output YOUR_OUTPUT_FLLE`


contribution:
    You can contribute to this package by raising bugs or sending pull requests
