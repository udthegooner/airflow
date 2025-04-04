from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
	'owner': 'cloud.user',
}

def inc(ctr):
	print(f"Count {ctr}")
	return ctr + 1

def mult(ti):
	ctr = ti.xcom_pull(task_ids='A')
	print(f"Count {ctr}")
	return ctr*100

def sub(ti):
	ctr = ti.xcom_pull(task_ids='B')
	print(f"Count {ctr}")
	return ctr - 9

def printt(ti):
	ctr = ti.xcom_pull(task_ids='C')
	print(f"Count {ctr}")

with DAG(
	dag_id = 'xcom',
	description = 'xcom',
	default_args = default_args,
	start_date = days_ago(1),
	schedule_interval = '@once',
	tags = ['xcom', 'python']
) as dag:
	
	A = PythonOperator(
		task_id = 'A',
		python_callable = inc,
		op_kwargs = {'ctr': 1}
	)

	B = PythonOperator(
		task_id = 'B',
		python_callable=mult,
	)

	C = PythonOperator(
		task_id = 'C',
		python_callable=sub,
	)

	D = PythonOperator(
		task_id = 'D',
		python_callable=printt
	)

A >> B >> C >> D