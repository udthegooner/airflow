from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

from random import choice

from datetime import date, datetime, timedelta
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'cloud.user',
}

def choose():
    return choice([True, False])

def branch(ti):
    if ti.xcom_pull(task_ids='choose'):
        return 'taskA'
    else:
        return 'taskB'

def A():
    print('(Py) Task A executed')

with DAG(
    dag_id = 'cron_catchup_backfill',
    description = 'Using cron, catchup, backfill',
    default_args = default_args,
    start_date = days_ago(30),
    schedule_interval = '0 */12 * * 6,0',
    catchup = True,
    tags = ['cron', 'catchup', 'backfill']
) as dag:

    task_choose = PythonOperator(
        task_id = 'choose',
        python_callable = choose
    )

    task_branch = BranchPythonOperator(
        task_id = 'branch',
        python_callable = branch
    )

    taskA = PythonOperator(
        task_id = 'taskA',
        python_callable = A
    )

    taskB = BashOperator(
        task_id = 'taskB',
        bash_command = 'echo "(Bash) Task D executed"'
    )

    taskC = EmptyOperator(
        task_id = 'taskC'
    )

task_choose >> task_branch >> [taskA, taskB]
taskA >> taskC
