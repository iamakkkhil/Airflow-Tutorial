"""
CHAPTER 4
"""

from datetime import datetime, timedelta

from airflow.decorators import task, dag
from airflow.operators.python import PythonOperator

"""
Task flow APIs will automatically calculates the dependencies
"""

default_args = {
    "owner": "Akhil",
    "retries": 5,
    "retry_delay": timedelta(minutes=2)
}


@dag(
    dag_id="taskflow_api_dag_v1",
    default_args=default_args,
    description="First Taskflow API DAG with Python Operator",
    start_date=datetime(2023, 7, 20, 2),
    schedule_interval="@daily",
)
def hello_world_etl():
    
    @task(multiple_outputs=True)
    def get_name():
        return {
            "first_name": "Akhil",
            "last_name": "Bhalerao"
        }
    
    @task()
    def get_age():
        return 21
    
    @task()
    def greet(first_name, last_name, age):
        print(f"Hello World, I am {first_name} {last_name}, I am {age} years old!")

    name_dict = get_name()
    age = get_age()
    greet(name_dict["first_name"], name_dict["last_name"], age)


# graph is created automatically we don't need to set upstream or downstream as set in Chapter 1, 2 and 3
greet_dag = hello_world_etl()