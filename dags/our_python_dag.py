"""
CHAPTER 2
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


default_args = {
    "owner": "Akhil",
    "retries": 5,
    "retry_delay": timedelta(minutes=2)
}


def greet():
    print("Hello World, from Python's DAG ;)")

def greet_by_name(name, age):
    print(f"Hello World, I am {name}, and I am {age} years old!")


with DAG(
    dag_id="python_dag_v1",
    default_args=default_args,
    description="First Python Dag with Python Operator",
    start_date=datetime(2023, 7, 20, 2),
    schedule_interval="@daily"
) as dg:
    task1 = PythonOperator(
        task_id="greet",
        python_callable=greet
    )

    task2 = PythonOperator(
        task_id="greet_by_me",
        python_callable=greet_by_name,
        op_kwargs={
            "name": "Akhil",
            "age": 21
        }
    )

    task1 >> task2