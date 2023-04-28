from collections import OrderedDict
from typing import Optional
from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb_sql.render.sqlalchemy_render import SqlalchemyRender

from mindsdb.integrations.libs.base import DatabaseHandler
from mindsdb.utilities import log
from mindsdb_sql import parse_sql
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE
import pandas as pd
import jaydebeapi as jd


class MaxDBHandler(DatabaseHandler):
    name = 'maxdb'

    def __init__(self, name: str, connection_data: Optional[dict], **kwargs):
        """ Initialize the handler
        Args:
            name (str): name of particular handler instance
            connection_data (dict): parameters for connecting to the database
            **kwargs: arbitrary keyword arguments.
        """
        super().__init__(name)
        self.kwargs = kwargs
        self.parser = parse_sql
        self.database = connection_data['database']
        self.connection_config = connection_data
        self.host = connection_data['host']
        self.port = connection_data['port']
        self.user = connection_data['user']
        self.password = connection_data['password']
        self.jdbcClass = 'com.sap.dbtech.jdbc.DriverSapDB'
        self.jdbcJarLocation = '/MindsDB/mindsdb/mindsdb/integrations/handlers/maxdb_handler/jdbc_driver/sapdbc.jar'
        self.connection = None
        self.is_connected = False

    def __del__(self):
        """
        Destructor for the Empress Embedded class.
        """
        if self.is_connected is True:
            self.disconnect()

    def connect(self) -> StatusResponse:
        """
        Establishes a connection to the Empress Embedded server.
        Returns:
            HandlerStatusResponse
        """
        if self.is_connected:
            return self.connection

        url = 'jdbc:sapdb://' + self.host + ':' + self.port + '/' + self.database

        # Open connection to database
        self.connection = jd.connect(self.jdbcClass, url, [self.user, self.password], jars=self.jdbcJarLocation)
        self.is_connected = True
        return self.connection

    def disconnect(self):
        """ Close any existing connections
        Should switch self.is_connected.
        """
        if self.is_connected is False:
            return
        try:
            self.connection.close()
            self.is_connected = False
        except Exception as e:
            log.logger.error(f"Error while disconnecting to {self.database}, {e}")

        return

    def check_connection(self) -> StatusResponse:
        """ Check connection to the handler
        Returns:
            HandlerStatusResponse
        """
        responseCode = StatusResponse(False)
        need_to_close = self.is_connected is False

        try:
            self.connect()
            responseCode.success = True
        except Exception as e:
            log.logger.error(f'Error connecting to database {self.database}, {e}!')
            responseCode.error_message = str(e)
        finally:
            if responseCode.success is True and need_to_close:
                self.disconnect()
            if responseCode.success is False and self.is_connected is True:
                self.is_connected = False

        return responseCode


    def native_query(self, query: str) -> Response:
        """
        Receive SQL query and runs it
        :param query: The SQL query to run in MySQL
        :return: returns the records from the current recordset
        """
        need_to_close = self.is_connected is False
        conn = self.connect()
        with conn.cursor() as cur:
            try:
                cur.execute(query)
                if cur.description:
                    result = cur.fetchall()
                    response = Response(
                        RESPONSE_TYPE.TABLE,
                        data_frame=pd.DataFrame(
                            result,
                            columns=[x[0] for x in cur.description]
                        )
                    )
                else:
                    response = Response(RESPONSE_TYPE.OK)
                self.connection.commit()
            except Exception as e:
                log.logger.error(f'Error running query: {query} on {self.database}!')
                response = Response(
                    RESPONSE_TYPE.ERROR,
                    error_message=str(e)
                )
                self.connection.rollback()

        if need_to_close is True:
            self.disconnect()

        return response
    def query(self, query: ASTNode) -> Response:
        """
        Retrieve the data from the SQL statement.
        """
        renderer = SqlalchemyRender('oracle')
        query_str = renderer.get_string(query, with_failback=True)
        return self.native_query(query_str)


    def get_tables(self, schema: str = None) -> Response:
        """
        Gets a list of table names in the database.
        Args:
            schema (str): The name of the schema to get tables from. If not provided,
                gets tables from all schemas in the database.
        Returns:
            list: A list of table names in the specified schema(s).
        """
        connection = self.connect()
        cursor = connection.cursor()
        # Execute query to get all table names from all schemas
        cursor.execute("SELECT TABLENAME FROM DOMAIN.TABLES WHERE SCHEMANAME='SYSINFO'")
        table_names = [x[0] for x in cursor.fetchall()]

        # Create dataframe with table names
        df = pd.DataFrame(table_names, columns=['table_name'])

        # Create response object
        response = Response(
            RESPONSE_TYPE.TABLE,
            df
        )

        return response

    def get_columns(self, table_name: str) -> Response:
        """
        Gets a list of column names in the specified table.
        Args:
            table_name (str): The name of the table to get column names from.
        Returns:
            list: A list of column names in the specified table.
        """
        q = "SELECT COLUMNNAME FROM DOMAIN.COLUMNS WHERE TABLENAME='{}'".format(table_name)
        result = self.native_query(q)
        return result



connection_args = OrderedDict(
        host={
            'type': ARG_TYPE.STR,
            'description': 'The host name or IP address of the Apache Derby server/database.'
        },
        port={
            'type': ARG_TYPE.INT,
            'description': 'Specify port to connect to Apache Derby.'
        },
        database={
            'type': ARG_TYPE.STR,
            'description': """
            The database name to use when connecting with the Apache Derby server.
        """
        },
        user={
            'type': ARG_TYPE.STR,
            'description': 'The user name used to authenticate with the Apache Derby server. If specified this is'
                           ' also treated as the schema.'
        },
        password={
            'type': ARG_TYPE.STR,
            'description': 'The password to authenticate the user with the Apache Derby server.'
        },

    )


connection_args_example = OrderedDict(
    host='localhost',
    port='7210',
    user='test',
    password='test',
    database="testdb",
    jdbcClass='com.sap.dbtech.jdbc.DriverSapDB',
    jdbcJarLocation='/Users/malid/Desktop/marsidmali-algo-assignments/assignment-2023-1/sapdbc.jar',
)
