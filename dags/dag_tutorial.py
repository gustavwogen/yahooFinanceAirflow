import os
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

from random import randint # Import to generate random numbers
def _training_model():
    print('CWD:', os.getcwd())
    print('listdir:', os.listdir('/opt/airflow/'))
    # foo = Variable.get("AZURE_DATABASE_NAME")
    # print('variable test =', foo)
    for env in sorted(os.environ.keys()):
        print(env)
    return randint(1, 10) # return an integer between 1 - 10


def _choosing_best_model(ti):
    accuracies = ti.xcom_pull(
        task_ids=[
            'training_model_A',
            'training_model_B',
            'training_model_C'
        ]
    )
    if max(accuracies) > 8:
        return 'accurate'
    return 'inaccurate'

default_args = {
    'owner': 'Airflow',
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



with DAG(
    "hello", # Dag id
    start_date=datetime(2021, 1 ,1), # start date, the 1st of January 2021 
    schedule_interval='@daily',  # Cron expression, here it is a preset of Airflow, @daily means once every day.
    catchup=False  # Catchup 
) as dag:

    # Tasks are implemented under the dag object
    training_model_tasks = [
        PythonOperator(
            task_id=f"training_model_{model_id}",
            python_callable=_training_model,
            op_kwargs={
                "model": model_id
            }
        ) for model_id in ['A', 'B', 'C']
    ]

    choosing_best_model = BranchPythonOperator(
        task_id="choosing_best_model",
        python_callable=_choosing_best_model
    )

    accurate = BashOperator(
        task_id="accurate",
        bash_command="echo 'accurate'"
    )

    inaccurate = BashOperator(
        task_id="inaccurate",
        bash_command="echo 'inaccurate'"
    )

    training_model_tasks >> choosing_best_model >> [accurate, inaccurate]
