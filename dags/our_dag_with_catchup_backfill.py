"""
CHAPTER 5
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

"""
In Catchup = True,  By deafult is True
DAG from Start Date till Today with the defined schedule_intervals
like startdate is 10th July 2023 and today is 21st July 2023 DAG will catchup and run for 10 Days.


In Catchup = False
It will do the exact opposite will not run for 10 Days only for single day that will be for today.

If Catchup = False
still want to run the catchup than run backfill in terminal by using below command

command: airflow dags backfill -s {start_date} -e {end_date} {dag_id}
example: airflow dags backfill -s 2023-07-01 -e 2023-07-08 dag_with_catchup_backfill_v2
"""

with DAG(
    dag_id="dag_with_catchup_backfill_v2",
    default_args=default_args,
    description="First Python Dag with Catchup and Backfill Operator",
    start_date=datetime(2023, 7, 10),
    schedule_interval="@daily",
    catchup=False
) as dg:
    task1 = PythonOperator(
        task_id="greet",
        python_callable=greet
    )

    task1