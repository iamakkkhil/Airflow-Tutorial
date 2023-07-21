"""
CHAPTER 3
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

"""
MAX XCOM SIZE is 48KB
"""


"""
In Apache Airflow, the ti (short for "task instance") is a context object that provides information and methods related to the 
currently executing task instance during the execution of a DAG. It is passed as an argument to the Python callable function 
that is executed by the task.

The ti object provides various attributes and methods that allow you to interact with the task instance and access information 
related to the current task run. Some of the commonly used attributes and methods of the ti object include:

1. ti.task: Returns the corresponding Task object for the task instance. You can use this to access information about the 
task definition, such as task ID, downstream tasks, etc.

2. ti.ds: Returns the execution date of the task instance in datetime format. This is the date for which the task is being 
executed.

3. ti.xcom_push(key, value): Allows you to push data (value) to XCom, which is a communication mechanism in Airflow to 
share data between tasks. The key is a string identifier for the data being pushed.

4. ti.xcom_pull(task_ids, key=None): Allows you to pull data from XCom of another task. You can specify the task_ids of 
the task from which you want to pull data and optionally provide a key to retrieve specific data if multiple pieces of data
are available.
"""


default_args = {
    "owner": "Akhil",
    "retries": 5,
    "retry_delay": timedelta(minutes=2)
}


def greet(ti):
    # Method 1
    # user_info = ti.xcom_pull(task_ids="get_name_age")
    # name = user_info.get("name")
    # age = user_info.get("age")

    # Method 2
    name = ti.xcom_pull(task_ids="get_name_age", key="name")
    age = ti.xcom_pull(task_ids="get_name_age", key="age")

    print(f"Hello World, I am {name}, and I am {age} years old!, from Python's DAG ;)")

def get_name_age(ti):
    # Method 1
    # return {"name": "Akhil", "age": 21}

    # Method 2
    ti.xcom_push(key="name", value="Akhil Bhalerao")
    ti.xcom_push(key="age", value="21")


with DAG(
    dag_id="xcom_dag_v2",
    default_args=default_args,
    description="First XCOM Dag with Python Operator providing data communication",
    start_date=datetime(2023, 7, 20, 2),
    schedule_interval="@daily"
) as dg:
    
    task0 = PythonOperator(
        task_id="get_name_age",
        python_callable=get_name_age
    )

    task1 = PythonOperator(
        task_id="greet",
        python_callable=greet
    )

    task0 >> task1