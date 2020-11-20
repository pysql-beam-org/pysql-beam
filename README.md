## pysql-beam

### This package is still under development but has been used in a few projects in production. It has been tested with dataflow Runner and Direct runner

This package provides Apache_beam io connectors for MySQL, MSSQL and Postgres databases.


This package provides apache beam io connectors for postgres db, mssql db and mysql db.
This package is a python implementation for those 3 io connectors

FYI: it uses a pyodbc connector for the mssql implementation, but not for the other two connectors  

Requirements:

    1. Python>=2.7 or python>= 3.5
    2. Apache beam >= 2.10
    3. pymysql[rsa]
    4. psycopg2-binary
    5. pyodbc


Installation:
    
    pip install git+git@github.com:jac2130/pysql-beam.git
   


Current functionality:

    1. Read from MySQL database by passing either table name or sql query
    2. Read from Postgres database by passing either table name or sql query
    3. Read from MSSQL database by passing either table name or squl query
    4. Write to BigQuery

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
ReadFromSQL(host=options.host, port=options.port,
        username=options.username, password=options.password,
        databse=options.database,
        query=options.source_query,
        wrapper=PostgresWrapper,
        batch=100000)

```
Examples:

    For mysql:
    `python cloud_sql_to_file.py --host localhost --port 3306 --database SECRET_DATABASE --username SECRET_USER --password SECRET_PASSWORD --table YOUR_TABLE --output_table 'MyProject:MyDataset.MyTable'  --temp_location "gs://MyBucket/tmp"`

    For postgres:
    `python cloud_sql_to_file.py --host localhost --port 5432 --database SECRET_DATABASE --username SECRET_USER --password SECRET_PASSWORD --table YOUR_TABLE --output_table 'MyProject:MyDataset.MyTable'  --temp_location "gs://MyBucket/tmp"`

    For mssql:
    `python cloud_sql_to_file.py --host localhost --port 1433 --database SECRET_DATABASE --username SECRET_USER --password SECRET_PASSWORD --query 'SELECT * from MyTable'  --output_table 'MyProject:MyDataset.MyTable'  --temp_location "gs://MyBucket/tmp"` 


contribution:
    You can contribute to this package by raising bugs or sending pull requests
