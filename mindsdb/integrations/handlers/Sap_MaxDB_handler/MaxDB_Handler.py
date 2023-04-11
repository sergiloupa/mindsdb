from typing import Optional
from collections import OrderedDict
from mindsdb.integrations.libs.base import DatabaseHandler
import pandas
import pyodbc
from mindsdb_sql import parse_sql
from mindsdb.utilities import log
from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)
#Check requirements.txt
import sdb.dbapi


class MindsDB_Handler(DatabaseHandler):
    """
    This Handler handles connection and execution of MindsDB statements.
    """
    name = 'maxdb'
    def __init__(self, name: str, connection_data: Optional[dict], **kwargs):
        """ constructor
        Args:
            name (str): the handler name
        """
        super().__init__(name)
        self.parser = parse_sql
        self.dialect = "maxdb"
        self.connection_data = connection_data
        self.kwargs = kwargs

        self.connection = None
        self.is_connected = False


    def connect(self):
        """Connect to a MaxDB database.
        Returns:
            Connection: The database connection.
        """
        if self.is_connected is True:
            return self.connection

        config = {
            'host' : self.connection_data.get('host'),
            'port': self.connection_data.get('port'),
            'user': self.connection_data.get('user'),
            'password': self.connection_data.get('password'),
            'database': self.connection_data.get('database')
        }
        try:
            connection = sdb.dbapi.connect(**config)
            self.is_connected = True
            self.connection = connection
        except Exception as e:
            log.logger.error(f"Error while connecting to MaxDB,{e}")

        return self.connection

    def __del__(self):
        if self.is_connected is True:
            self.disconnect()

    def disconnect(self):
        """Close the database connection."""
        if self.is_connected is False:
            return
        try:
            self.connection.close()
            self.is_connected = False
        except Exception as e:
            log.logger.error(f"Error while disconnecting from MaxDB, {e}")

        return

    def check_connection(self):
        """Check the connection to the MaxDB database.
        Returns:
            StatusResponse: Connection success status and error message if an error occurs.
        """
        response = StatusResponse(False)
        need_to_close = self.is_connected is False

        try:
            self.connect()
            response.success = True
        except Exception as e:
            log.logger.error(f'Error connecting to MaxDB, {e}!')
            response.error_message = str(e)
        finally:
            if response.success is True and need_to_close:
                self.disconnect()
            if response.success is False and self.is_connected is True:
                self.is_connected = False

        return response

    def native_query(self, query: str):
        """Execute a SQL query.
        Args:
            query (str): The SQL query to execute.
        Returns:
            Response: The query result.
        """
        need_to_close = self.is_connected is False
        connection = self.connect()
        cur = connection.cursor()
        try:
            cur.execute(query)
            "We check if the cursor has rows available for fetching or not."
            if cur.with_rows:
                result = cur.fetchall()
                response = Response(
                    RESPONSE_TYPE.TABLE,
                    pandas.DataFrame(
                        result,
                        columns=[x[0] for x in cur.description]
                    )
                )
            else:
                response = Response(RESPONSE_TYPE.OK)
        except Exception as e:
            log.logger.error(f'Error running query: {query} on MaxDB!')
            response = Response(
                RESPONSE_TYPE.ERROR,
                error_message=str(e)
            )
        cur.close()
        if need_to_close is True:
            self.disconnect()

        return response

    def query(self, query: ASTNode):
        """Receive query as AST (abstract syntax tree) and act upon it.
        """
        if isinstance(query, ASTNode):
            query_str = query.to_string()
        else:
            query_str = str(query)

        return self.native_query(query_str)


    def get_tables(self):
        """Get a list of all the tables in the database.
        Returns:
            Response: Names of the tables in the database.
        """
        q = "SHOW TABLES;"
        result = self.native_query(q)
        df = result.data_frame
        result.data_frame = df.rename(columns={df.columns[0]: 'table_name'})
        return result

    def get_columns(self, table_name: str):
        """Get details about a table.
        Args:
            table_name (str): Name of the table to retrieve details of.
        Returns:
            Response: Details of the table.
        """
        q = f"DESCRIBE {table_name};"
        result = self.native_query(q)
        return result


connection_args = OrderedDict(
    user = {
        'type': ARG_TYPE.STR,
        'description': 'The user name used to authenticate with the MaxDB server.'
    },
    password = {
        'type': ARG_TYPE.STR,
        'description': 'The password to authenticate the user with the MaxDB server.'
    },
    database = {
        'type': ARG_TYPE.STR,
        'description': 'The database name to use when connecting with the MaxDB server.'
    },
    host = {
        'type': ARG_TYPE.STR,
        'description': 'The host name or IP address of the MaxDB server.'
    },
    port = {
        'type': ARG_TYPE.INT,
        'description': 'The TCP/IP port of the MaxDB server. Must be an integer.'
    }
)
connection_args_example = OrderedDict(
    host = '127.0.0.1',
    port = 7210,
    user = 'root',
    password = 'password',
    database = 'database'
)