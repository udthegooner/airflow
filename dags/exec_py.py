from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
	'owner': 'cloud.user',
}

def A(name):
	print(f"hello {name}")

def B(city):
	print(f"hello {city}")

def C():
	print("hello C")

def D():
	print("hello D")

with DAG(
	dag_id = 'exec_py',
	description = 'first py dag',
	default_args = default_args,
	start_date = days_ago(1),
	schedule_interval = '@once',
	tags = ['beginner', 'python', 'hw']
) as dag:

	taskA = PythonOperator(
		task_id = 'taskA_hw_py',
		python_callable = A,
		op_kwargs = {"name": "all"}
	)

	taskB = PythonOperator(
		task_id = 'taskB_hw_py',
		python_callable = B,
		op_kwargs = {"city": "rio"}
	)

	# taskC = PythonOperator(
	# 	task_id = 'taskC_hw_py',
	# 	python_callable = C
	# )

	# taskD = PythonOperator(
	# 	task_id = 'taskD_hw_py',
	# 	python_callable = D
	# )


# taskA >> [taskB, taskC]
# taskD << [taskB, taskC]
taskA >> taskB