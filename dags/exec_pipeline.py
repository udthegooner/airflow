import pandas as pd

from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
	'owner': 'cloud.user',
}

def read_csv_file():
	df = pd.read_csv('/home/udhbhav/airflow/datasets/insurance.csv')
	print(df)
	return df.to_json()

def remove_null(**kwargs):
	ti = kwargs['ti']
	json_data = ti.xcom_pull(task_ids='read')
	df = pd.read_json(json_data)
	df = df.dropna()

	print(df)

	return df.to_json()

def groupby_smoker(ti):
	data = ti.xcom_pull(task_ids='remove_null')
	df = pd.read_json(data)

	# group by smoker and calc avg age, bmi, insurance charges
	smoker_df = df.groupby('smoker').agg({
		'age': 'mean',
		'bmi': 'mean',
		'charges': 'mean'
	}).reset_index()

	smoker_df.to_csv("/home/udhbhav/airflow/output/smoker.csv")

def groupby_region(ti):
	data = ti.xcom_pull(task_ids='remove_null')
	df = pd.read_json(data)

	# group by region and calc avg age, bmi, insurance charges
	region_df = df.groupby('region').agg({
		'age': 'mean',
		'bmi': 'mean',
		'charges': 'mean'
	}).reset_index()

	region_df.to_csv("/home/udhbhav/airflow/output/region.csv")


with DAG(
	dag_id = 'pipeline',
	description = 'simple pipeline',
	default_args = default_args,
	start_date = days_ago(1),
	schedule_interval = '@once',
	tags = ['xcom', 'python', 'pipeline']
) as dag:
	
	read = PythonOperator(
		task_id = 'read',
		python_callable = read_csv_file
	)

	remove_null = PythonOperator(
		task_id = 'remove_null',
		python_callable = remove_null
	)

	groupby_smoker = PythonOperator(
		task_id = 'gpSmoker',
		python_callable = groupby_smoker
	)

	groupby_region = PythonOperator(
		task_id = 'gpRegion',
		python_callable = groupby_region
	)

read >> remove_null >> [groupby_region, groupby_smoker]