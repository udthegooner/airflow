import pandas as pd
from random import choice
from datetime import date, datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.dates import days_ago

from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from airflow.utils.edgemodifier import Label

default_args = {
    'owner': 'cloud.user',
}

OUTPUT_PATH = "/home/udhbhav/airflow/output/{0}.csv"

def read_csv_file(ti):
    df = pd.read_csv('/home/udhbhav/airflow/datasets/insurance.csv')
    print(df)
    ti.xcom_push(key='insurance_csv', value=df.to_json())

def remove_null_values(ti):
    json_data = ti.xcom_pull(key='insurance_csv')

    df = pd.read_json(json_data)
    df = df.dropna()

    print(df)
    ti.xcom_push(key = 'clean_csv', value=df.to_json())

def branch():
    transform_action = Variable.get("transform_action", default_var = None)

    if transform_action.startswith('filter'):
        return "filtering.{0}".format(transform_action)
    elif transform_action == 'groupby_region_smoker':
        return "grouping.{0}".format(transform_action)

def filter_by_southwest(ti):
    json_data = ti.xcom_pull(key='clean_csv')
    df = pd.read_json(json_data)

    southwest_df = df[df['region'] == 'southwest']

    southwest_df.to_csv(OUTPUT_PATH.format('southwest'), index=False)

def filter_by_southeast(ti):
    json_data = ti.xcom_pull(key='clean_csv')
    df = pd.read_json(json_data)

    southeast_df = df[df['region'] == 'southeast']

    southeast_df.to_csv(OUTPUT_PATH.format('southeast'), index=False)

def groupby_region_smoker(ti):
    data = ti.xcom_pull(key='clean_csv')
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
    
    with TaskGroup('read_preprocess') as read_preprocess:
        read = PythonOperator(
            task_id = 'read',
            python_callable = read_csv_file
        )

        clean = PythonOperator(
            task_id = 'remove_null',
            python_callable = remove_null_values
        )

        read >> clean

    branch = BranchPythonOperator(
        task_id = 'branch',
        python_callable = branch
    )

    with TaskGroup("filtering") as filtering:
        filter_by_southeast = PythonOperator(
            task_id = 'filter_by_southeast',
            python_callable = filter_by_southeast
        )

        filter_by_southwest = PythonOperator(
            task_id = 'filter_by_southwest',
            python_callable = filter_by_southwest
        )

    with TaskGroup("grouping") as grouping:
        groupby_region_smoker = PythonOperator(
            task_id = 'groupby_region_smoker',
            python_callable = groupby_region_smoker
        )

    read_preprocess >> Label('preprocessed_data') >> branch >> Label('branch on condition variable') >> [filtering, grouping]