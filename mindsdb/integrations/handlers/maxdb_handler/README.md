# Sap MaxDB Handler

This is the implementation of the Sap MaxDB handler for MindsDB.

## Sap MaxDB (What is it?)
SAP MaxDB is a high-performance, scalable, and reliable relational database management system (RDBMS) that supports a wide range of applications. It is designed to handle large amounts of data with minimal downtime and maximum availability. MaxDB provides advanced features such as backup and recovery, high availability, and online data compression, making it a popular choice for enterprise applications.


## Implementation
The MaxDB handler uses the JDBC driver to connect to the MaxDB database. Specifically, it uses the sapdbc.jar file as the driver to establish the connection. To use the MaxDB handler in Python, we use the jaydebeapi module to establish a connection to the database.

To establish a connection with SAP MaxDB, the following arguments are required:
* `host`: IP address of the computer where the database server is running.
* `port`: The number used by the operating system to identify a specific process or service on the server.
* `user`: Username used to authenticate and authorize access to a specific database.
* `password`: Secret authentication credential that is associated with a specific user account.
* `database`: Database name to be connected.

## Usage
In order to make use of this handler and connect to MaxDB in MindsDB, the following syntax can be used,
~~~~sql
CREATE DATABASE maxdb_datasource
WITH engine='maxdb',
parameters={
    "host": "localhost",
    "port": "7210",
    "user": "username",
    "password": "password",
    "database": "DatabaseName"
};
~~~~

Now, you can use this established connection to query your database as follows,
~~~~sql
SELECT * FROM maxdb_datasource.TESTTABLE;
~~~~