import unittest
from mindsdb.integrations.handlers.maxdb_handler.maxdb_handler import MaxDBHandler
from mindsdb.api.mysql.mysql_proxy.libs.constants.response_type import RESPONSE_TYPE


class MaxDBHandlerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.kwargs = {
            "connection_data": {
                "host": "192.168.1.3",
                "port": "7210",
                "user": "DBADMIN",
                "password": "Asdfg4546",
                "database": "MaxDB"
            }
        }
        cls.handler = MaxDBHandler('test_maxdb_handler', **cls.kwargs)

    def test_0_connect(self):
        self.handler.connect()

    def test_1_check_connection(self):
        self.handler.check_connection()

    def test_2_create(self):
        res = self.handler.query('CREATE TABLE TESTTABLEX (ID INT PRIMARY KEY, NAME VARCHAR(14))')
        assert res.type is RESPONSE_TYPE.OK

    def test_3_insert(self):
        res = self.handler.query("INSERT INTO TESTTABLEX VALUES (100,'ONE HUNDRED'),(200,'TWO HUNDRED'),(300,'THREE HUNDRED')")
        assert res.type is RESPONSE_TYPE.OK

    def test_4_select(self):
        res = self.handler.query('SELECT * FROM TESTTABLEX')
        assert res.type is RESPONSE_TYPE.TABLE
    
    def test_5_get_tables(self):
        res = self.handler.get_tables()
        assert res.type is RESPONSE_TYPE.TABLE

    def test_6_get_columns(self):
        res = self.handler.get_columns("TESTTABLEX")
        assert res.type is RESPONSE_TYPE.TABLE

if __name__ == '__main__':
    unittest.main()
