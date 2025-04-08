from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator

from random import choice

from datetime import date, datetime, timedelta
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'cloud.user',
}

def has_driving_license():
    return choice([True, False])

def branch(ti):
    if ti.xcom_pull(task_ids='has_dl'):
        return 'eligible'
    else:
        return 'ineligible'

def eligible():
    print('You can drive')

def ineligible():
    print("Can't drive")

with DAG(
    dag_id = 'exec_branch',
    description = 'branching',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '@once',
    tags = ['branching', 'python']
) as dag:
    
    has_dl = PythonOperator(
        task_id = 'has_dl',
        python_callable = has_driving_license
    )

    task_branch = BranchPythonOperator(
        task_id = 'branch',
        python_callable = branch
    )

    eligible = PythonOperator(
        task_id = 'eligible',
        python_callable = eligible
    )

    ineligible = PythonOperator(
        task_id = 'ineligible',
        python_callable = ineligible
    )

has_dl >> task_branch >> [eligible, ineligible]

