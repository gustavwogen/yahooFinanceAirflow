import json
import os
from airflow.models.connection import Connection
from airflow.providers.odbc.hooks.odbc import OdbcHook
from airflow.providers.common.sql.hooks.sql import DbApiHook
from airflow.models import Variable
import sqlalchemy
import pyodbc



hook = OdbcHook(odbc_conn_id="projects_conn")

print(hook.get_uri())
print()
print(hook.odbc_connection_string)

