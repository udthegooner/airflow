from airflow import DAG

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from datetime import date, datetime, timedelta
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'cloud.user',
}

with DAG(
    dag_id = 'exec_sql_pipeline',
    description = 'sql pipeline',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '@once',
    tags = ['pipeline', 'sql']
) as dag:
    create_table = SQLExecuteQueryOperator(
        task_id = 'create_table',
        sql = r"""
            CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY,
                    name VARCHAR(50) NOT NULL,
                    age INTEGER NOT NULL,
                    city VARCHAR(50),
                    is_active BOOLEAN DEFAULT true,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """,
        conn_id = 'sqlite_conn',
        dag = dag,
    )

    insert1 = SQLExecuteQueryOperator(
        task_id = 'insert1',
        sql = r"""
        INSERT INTO users (name, age, is_active) VALUES 
                ('Julie', 30, false),
                ('Peter', 55, true),
                ('Emily', 37, false),
                ('Katrina', 54, false),
                ('Joseph', 27, true);
        """,
        conn_id = 'sqlite_conn',
        dag = dag,
    )

    insert2 = SQLExecuteQueryOperator(
        task_id = 'insert2',
        sql = r"""
        INSERT INTO users (name, age) VALUES 
                ('Harry', 49),
                ('Nancy', 52),
                ('Elvis', 26),
                ('Mia', 20);
        """,
        conn_id = 'sqlite_conn',
        dag = dag,
    )

    delete = SQLExecuteQueryOperator(
        task_id = 'delete',
        sql = r"""
            DELETE FROM users WHERE is_active = 0;
        """,
        conn_id = 'sqlite_conn',
    )

    update = SQLExecuteQueryOperator(
        task_id = 'update',
        sql = r"""
            UPDATE users SET city = 'Seattle';
        """,
        conn_id = 'sqlite_conn',
    )

    display = SQLExecuteQueryOperator(
        task_id = 'display',
        sql = r"""Select * from users;""",
        conn_id = 'sqlite_conn',
        do_xcom_push = True
    )

create_table >> [insert1, insert2] >> delete >> update >> display

