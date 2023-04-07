from typing import Optional
from collections import OrderedDict
from mindsdb.integrations.libs.base import DatabaseHandler
import MaxDB.connector
import pandas
from mindsdb_sql import parse_sql
from mindsdb.utilities import log
from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)
from mindsdb_sql.render.sqlalchemy_render import SqlalchemyRender

class MindsDB_Hanlder(DatabaseHandler):
    """
    This Handler handles connection and execution of MindsDB statements.
    """
    def __init__(self, name: str, connection_data: Optional[dict], **kwargs):
        """ constructor
        Args:
            name (str): the handler name
        """
        super().__init__(name)
        self.parser = parse_sql
        self.dialect = 'maxdb'
        self.connection_data = connection_data
        self.kwargs = kwargs

        self.connection = None
        self.is_connected = False


    def connect(self):
        """ Set up any connections required by the handler
        Should return output of check_connection() method after attempting
        connection. Should switch self.is_connected.
        Returns:
            HandlerStatusResponse
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
        ssl = self.connection_data.get('ssl')
        if ssl is True:
            ssl_ca = self.connection_data.get('ssl_ca')
            ssl_cert = self.connection_data.get('ssl_cert')
            ssl_key = self.connection_data.get('ssl_key')
            config['client_flags'] = [MaxDB.connector.constants.ClientFlag.SSL]
            if ssl_ca is not None:
                config["ssl_ca"] = ssl_ca
            if ssl_cert is not None:
                config["ssl_cert"] = ssl_cert
            if ssl_key is not None:
                config["ssl_key"] = ssl_key

        connection = MaxDB.connector.connect(**config)
        self.is_connected = True
        self.connection = connection
        return self.connection

    def __del__(self):
        if self.is_connected is True:
            self.disconnect()

    def disconnect(self):
        """ Close any existing connections
        Should switch self.is_connected.
        """
        if self.is_connected is False:
            return
        self.connection.close()
        self.is_connected = False
        return

    def check_connection(self):
        """ Check connection to the handler
        Returns:
            HandlerStatusResponse
        """
        response = StatusResponse(False)
        need_to_close = self.is_connected is False

        try:
            self.connect()
            response.success = True
        except Exception as e:
            log.logger.error(f'Error connecting to Redshift {self.connection_data["database"]}, {e}!')
            response.error_message = str(e)
        finally:
            if response.success is True and need_to_close:
                self.disconnect()
            if response.success is False and self.is_connected is True:
                self.is_connected = False

        return response

    def native_query(self, query: str):
        """Receive raw query and act upon it somehow.
        Args:
            query (Any): query in native format (str for sql databases,
                dict for mongo, etc)
        Returns:
            HandlerResponse
        """
        need_to_close = self.is_connected is False

        connection = self.connect()
        with connection.cursor(dictionary=True, buffered=True) as cur:
            try:
                cur.execute(query)
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
                connection.commit()
            except Exception as e:
                log.logger.error(f'Error running query: {query} on {self.connection_data["database"]}!')
                response = Response(
                    RESPONSE_TYPE.ERROR,
                    error_message=str(e)
                )
                connection.rollback()

        if need_to_close is True:
            self.disconnect()

        return response

    def query(self, query: ASTNode):
        """Receive query as AST (abstract syntax tree) and act upon it somehow.
        Args:
            query (ASTNode): sql query represented as AST. May be any kind
                of query: SELECT, INSERT, DELETE, etc
        Returns:
            HandlerResponse
        """
        renderer = SqlalchemyRender('maxdb')
        query_str = renderer.get_string(query, with_failback=True)
        return self.native_query(query_str)


    def get_tables(self):
        """ Return list of entities
        Return list of entities that will be accesible as tables.
        Returns:
            HandlerResponse: shoud have same columns as information_schema.tables
                (https://dev.mysql.com/doc/refman/8.0/en/information-schema-tables-table.html)
                Column 'TABLE_NAME' is mandatory, other is optional.
        """
        q = "SHOW TABLES;"
        result = self.native_query(q)
        df = result.data_frame
        result.data_frame = df.rename(columns={df.columns[0]: 'table_name'})
        return result

    def get_columns(self, table_name: str):
        """ Returns a list of entity columns
        Args:
            table_name (str): name of one of tables returned by self.get_tables()
        Returns:
            HandlerResponse: shoud have same columns as information_schema.columns
                (https://dev.mysql.com/doc/refman/8.0/en/information-schema-columns-table.html)
                Column 'COLUMN_NAME' is mandatory, other is optional. Hightly
                recomended to define also 'DATA_TYPE': it should be one of
                python data types (by default it str).
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
        'description': 'The host name or IP address of the MaxDB server. NOTE: use \'127.0.0.1\' instead of \'localhost\' to connect to local server.'
    },
    port = {
        'type': ARG_TYPE.INT,
        'description': 'The TCP/IP port of the MaxDB server. Must be an integer.'
    },
    ssl = {
        'type': ARG_TYPE.BOOL,
        'description': 'Set it to False to disable ssl.'
    },
    ssl_ca = {
        'type': ARG_TYPE.PATH,
        'description': 'Path or URL of the Certificate Authority (CA) certificate file'
    },
    ssl_cert = {
        'type': ARG_TYPE.PATH,
        'description': 'Path name or URL of the server public key certificate file'
    },
    ssl_key = {
        'type': ARG_TYPE.PATH,
        'description': 'The path name or URL of the server private key file'
    }
)
connection_args_example = OrderedDict(
    host = '127.0.0.1',
    port = 3306,
    user = 'root',
    password = 'password',
    database = 'database'
)