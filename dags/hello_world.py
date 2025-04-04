# for scheduling
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG

from airflow.operators.bash import BashOperator

default_args = {
	'owner': 'cloud.user',
}

with DAG(
	dag_id = 'hw',
	description = 'first dag',
	default_args = default_args,
	start_date = days_ago(1),
	schedule_interval = '@daily',
	tags = ['beginner', 'bash', 'hw']
) as dag:

	task = BashOperator(
		task_id = 'task_hw_with',
		bash_command = 'echo "Hello World"',
	)

task