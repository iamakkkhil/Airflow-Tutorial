"""
CHAPTER 1
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    "owner": "Akhil",
    "retries": 5,
    "retry_delay": timedelta(minutes=2)
}


with DAG(
    dag_id="our_first_dag_v2",
    default_args=default_args,
    description="First Airflow Dag that I write",
    start_date=datetime(2023, 7, 20, 2),
    schedule_interval="@daily",
) as dag:
    task1 = BashOperator(
        task_id="First_Task",
        bash_command="echo Hello World, From Airflow, This is the first Task"
    )

    task2 = BashOperator(
        task_id="Second_Task",
        bash_command="echo Hello World, From Airflow, This is the Second Task 2222"
    )

    task3 = BashOperator(
        task_id="Third_Task",
        bash_command="echo Hey I am task 3, will be running same time as task 2"
    )

    # Method 1
    # task1.set_downstream(task2)
    # task1.set_downstream(task3)

    #Method 2
    # task1 >> task2
    # task1 >> task3

    #Method 3
    task1 >> [task2, task3]

