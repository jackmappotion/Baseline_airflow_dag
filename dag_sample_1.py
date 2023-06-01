from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator


def _task_1():
    print("this is task 1")


def _task_2():
    print("this is task 2")


dag = DAG(
    "Hellworld_DAG",
    schedule_interval=None,
    start_date=datetime(2023, 6, 1),
    catchup=False,
)

dummy_task = DummyOperator(
    task_id="dummy_task",
	dag=dag
    )
task_1 = PythonOperator(
    task_id="task_1",
    python_callable=_task_1,
    dag=dag
    )
task_2 = PythonOperator(
    task_id="task_2",
    python_callable=_task_2,
    dag=dag
    )

dummy_task >> [task_1,task_2]

