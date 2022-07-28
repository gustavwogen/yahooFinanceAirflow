import os
import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.sensors.http import  HttpSensor
from airflow.providers.odbc.hooks.odbc import OdbcHook
from airflow.hooks.dbapi import DbApiHook
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from plugins.operators.yahoo import ProcessYahooAPIOperator
from plugins.orms.finance import FinanceData
from datetime import datetime, timedelta


API_KEY_YAHOO = os.environ.get('API_KEY_YAHOO')



default_args = {
    'owner': 'Gurrastav',
    'depends_on_past': False,
    'start_date': datetime(2022, 7, 14),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

symbols = [
    'AAPL',
    # 'MSFT',
    # 'TSLA'
]
params = {
    "region": "US", 
    "lang": "en", 
    "symbols": ','.join(symbols)
}


def handle_response(response, endpoint):
    if response.status_code == 200:
        print(f'Response from {endpoint} was 200')
        return True
    print(f"Response from {endpoint} was {response.status_code}")
    return False


def insert_to_db(task_instance, table):
    data = task_instance.xcom_pull(task_ids='process_finance_data')
    print(data)
    assert type(data) is list, "Type for data must be list"

    if not all(type(element) == dict for element in data):
        print('All elements in data must be of type dict')
        return

    columns = set([col.name for col in list(table.__table__.columns)])
    for key in data[0].keys():
        if key not in columns:
            print('Incorrect columns')
            return

    hook = OdbcHook(odbc_conn_id="projects_conn")
    engine = hook.get_sqlalchemy_engine()

    from sqlalchemy import inspect, insert

    tables = inspect(engine).get_table_names()
    print('tables in db:', tables)
    print('this table:', table.__table__.name)
    if table.__table__.name not in tables:
        table.__table__.create(engine)

    with engine.begin() as conn:
        for row in data:
            if not row:
                continue
            if 'lastTradeUTC' in row:
                row['lastTradeUTC'] = datetime.utcfromtimestamp(row['lastTradeUTC'])
            stmt = insert(table).values(**row)
            conn.execute(stmt)



with DAG(
    "YahooFinanceWorkflow", # Dag id
    start_date=datetime(2022, 7, 20), # start date
    schedule_interval='@once',  # Cron expression, here it is a preset of Airflow, @daily means once every day.
    catchup=False  # Catchup 
) as dag:
    endpoint = '/v6/finance/quote'

    def handling_response(task_instance):
        data = task_instance.xcom_pull(task_ids='test_query')
        print('data:', data)
        return data

    def sample_select():
        conn_id = 'test_odbc'
        odbc_hook = OdbcHook.get_connection(conn_id=conn_id) 
        conn = odbc_hook.get_conn()
        print('conn:', conn)

    task_is_api_active = HttpSensor(
        task_id='is_api_active',
        http_conn_id='yahoo_finance_api',
        endpoint=endpoint,
        request_params=params,
        headers={
            'x-api-key': API_KEY_YAHOO
        },
        timeout=60 * 30,
        response_check=lambda response: handle_response(response, endpoint)
    )

    query_data_from_api = SimpleHttpOperator(
        task_id='query_data',
        http_conn_id='yahoo_finance_api',
        endpoint=endpoint,
        method='GET',
        data=params,
        headers={
            'x-api-key': API_KEY_YAHOO
        },
        response_check=lambda response: handle_response(response, endpoint)
    )

    custom_operator = ProcessYahooAPIOperator(
        task_id='process_finance_data',
        columns_of_interest={
            'symbol': 'symbol',
            'open': 'regularMarketOpen',
            'close': 'regularMarketPrice',
            'high': 'regularMarketDayHigh',
            'low': 'regularMarketDayLow',
            'change': 'regularMarketChange',
            'changePercent': 'regularMarketChangePercent',
            'twoHundredDayAverage': 'twoHundredDayAverage',
            'twoHundredDayAverageChange': 'twoHundredDayAverageChange',
            'twoHundredDayAverageChangePercent': 'twoHundredDayAverageChangePercent'
        }
    )

    insert_data = PythonOperator(
        task_id='print_data',
        python_callable=insert_to_db,
        op_kwargs={'table': FinanceData}
    )

    task_is_api_active >> query_data_from_api >> custom_operator >> insert_data
