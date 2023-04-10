## Sap MaxDB
This is the implementation of the Sap MaxDB handler for MindsDB.

## Sap MaxDB (What is it?)
SAP MaxDB is a high-performance, scalable, and reliable database management system that supports a wide range of applications. It is designed to handle large amounts of data with minimal downtime and maximum availability. MaxDB provides advanced features such as backup and recovery, high availability, and online data compression, making it a popular choice for enterprise applications.

## Implementation
To establish a connection with SAP MaxDB, the following arguments are required:

* host: the hostname or IP address of the SAP MaxDB server
* port: the port number to use when connecting to the server
* user: the user to authenticate
* password: the password to authenticate the user
* database: the name of the database to connect to

Once a connection is established, users can leverage the power of MindsDB's automated machine learning to easily generate accurate predictions on their SAP MaxDB data. The handler supports various features such as custom queries, column selection, and row filtering to provide a seamless integration with SAP MaxDB databases.

## Usage
In order to make use of this handler and connect to a MaxDB database in MindsDB, the following syntax can be used,
```sql
CREATE DATABASE MaxDB_datasource
WITH
engine='maxdb',
parameters={
    "host": "127.0.0.1",
    "port": "7210",
    "user": "user",
    "password": "pass",
    "database": "db_name"
};
```
Now, you can use this established connection to query your database as follows,
```sql
SELECT * FROM MaxDB_datasource.example_tbl
```