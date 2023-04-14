import unittest
from mindsdb.integrations.handlers.Sap_MaxDB_handler import MaxDB_Handler
from mindsdb.api.mysql.mysql_proxy.libs.constants.response_type import RESPONSE_TYPE

class MaxDBHandlerTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.kwargs = {
            "connection_data": {
                'host': '192.168.1.182',
                'port': '7210',
                'user': 'DBADMIN',
                'password': 'Asdfg4546',
                'database': 'maxdb'
            }
        }
        cls.handler = MaxDB_Handler('test_maxdb_handler', **cls.kwargs)


if __name__ == '__main__':
    unittest.main()