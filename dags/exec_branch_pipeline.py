from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
import pandas as pd
from random import choice

from datetime import date, datetime, timedelta
from airflow.utils.dates import days_ago
from airflow.models import Variable

default_args = {
    'owner': 'cloud.user',
}

OUTPUT_PATH = "/home/udhbhav/airflow/output/{0}.csv"

def read_csv_file():
    df = pd.read_csv('/home/udhbhav/airflow/datasets/insurance.csv')
    print(df)
    return df.to_json()

def remove_null_values(ti):
    json_data = ti.xcom_pull(task_ids='read')
    df = pd.read_json(json_data)
    df = df.dropna()

    print(df)

    return df.to_json()

def branch():
    transform_action = Variable.get("transform_action", default_var = None)

    if transform_action.startswith('filter'):
        return transform_action
    elif transform_action == 'groupby_region_smoker':
        return 'groupby_region_smoker'

def filter_by_southwest(ti):
    json_data = ti.xcom_pull(task_ids = 'remove_null')
    df = pd.read_json(json_data)

    southwest_df = df[df['region'] == 'southwest']

    southwest_df.to_csv(OUTPUT_PATH.format('southwest'), index=False)

def filter_by_southeast(ti):
    json_data = ti.xcom_pull(task_ids = 'remove_null')
    df = pd.read_json(json_data)

    southeast_df = df[df['region'] == 'southeast']

    southeast_df.to_csv(OUTPUT_PATH.format('southeast'), index=False)

def groupby_region_smoker(ti):
    data = ti.xcom_pull(task_ids='remove_null')
    df = pd.read_json(data)

    # group by smoker and calc avg age, bmi, insurance charges
    smoker_df = df.groupby('smoker').agg({
        'age': 'mean',
        'bmi': 'mean',
        'charges': 'mean'
    }).reset_index()

    smoker_df.to_csv(OUTPUT_PATH.format('smoker'), index=False)

    # group by region and calc avg age, bmi, insurance charges
    region_df = df.groupby('region').agg({
        'age': 'mean',
        'bmi': 'mean',
        'charges': 'mean'
    }).reset_index()

    region_df.to_csv(OUTPUT_PATH.format('region'), index=False)

with DAG(
    dag_id = 'exec_branch_pipeline',
    description = 'branching pipeline',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '@once',
    tags = ['branching', 'python', 'pipeline']
) as dag:

    read = PythonOperator(
        task_id = 'read',
        python_callable = read_csv_file
    )

    clean = PythonOperator(
        task_id = 'remove_null',
        python_callable = remove_null_values
    )

    branch = BranchPythonOperator(
        task_id = 'branch',
        python_callable = branch
    )

    filter_by_southeast = PythonOperator(
        task_id = 'filter_by_southeast',
        python_callable = filter_by_southeast
    )

    filter_by_southwest = PythonOperator(
        task_id = 'filter_by_southwest',
        python_callable = filter_by_southwest
    )

    groupby_region_smoker = PythonOperator(
        task_id = 'groupby_region_smoker',
        python_callable = groupby_region_smoker
    )

read >> clean >> branch >> [filter_by_southeast, filter_by_southwest, groupby_region_smoker]