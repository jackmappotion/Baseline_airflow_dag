from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator


def task_1():
    print("This is task 1")


def task_2():
    print("This is task 2")


with DAG(
    "Helloworld_DAG_refactored",
    schedule_interval=None,
    start_date=datetime(2023, 6, 1),
    catchup=False,
) as dag:
    dummy_task = DummyOperator(task_id="dummy_task")
    task_1 = PythonOperator(task_id="task_1", python_callable=task_1)
    task_2 = PythonOperator(task_id="task_2", python_callable=task_2)

    dummy_task >> [task_1, task_2]
