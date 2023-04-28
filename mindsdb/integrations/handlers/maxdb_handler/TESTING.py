from mindsdb.integrations.handlers.maxdb_handler.maxdb_handler import MaxDBHandler
import jaydebeapi

jdbc_driver = 'com.sap.dbtech.jdbc.DriverSapDB'
url = 'jdbc:sapdb://192.168.1.182:7210/MaxDB'
user = 'DBADMIN'
password = 'Asdfg4546'
jars = '/Users/falin/PycharmProjects/testing/libs/sapdbc.jar'

# Open connection to database
conn = jaydebeapi.connect(jdbc_driver, url, [user, password], jars=jars)
cursor = conn.cursor()

table_name = 'ACTIVECONFIGURATION'
cursor.execute("SELECT COLUMNNAME FROM DOMAIN.COLUMNS WHERE TABLENAME='{}'".format(table_name))
tables = cursor.fetchall()
print(tables)

# close the cursor and connection
cursor.close()
conn.close()