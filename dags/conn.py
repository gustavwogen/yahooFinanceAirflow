import json
import os
from airflow.models.connection import Connection
from airflow.providers.odbc.hooks.odbc import OdbcHook
from airflow.providers.common.sql.hooks.sql import DbApiHook
from airflow.models import Variable
import sqlalchemy
import pyodbc





# with conn.get_sqlalchemy_connection() as conn:
#     print('hello')

# conn.conn_name_attr = 'odbc_test'
# print('conn attr:', conn.connection)
# for key, val in dict(sorted(os.environ.items())).items():
#     print(f"{key}: {val}")
# s = conn.odbc_connection_string()
# print(s)



# pyodbc.connect('DSN=MSSQLServerDatabase;UID=myuid;PWD=mypwd')
