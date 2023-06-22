import pendulum
from datetime import datetime
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator


BASE_DIR = os.path.dirname(__file__)

KST = pendulum.timezone("Asia/Seoul")
args = {'owner': 'kurrant'}

dag  = DAG(dag_id='my_dag',
        default_args=args,
        start_date=datetime(2021, 2, 8, tzinfo=KST),
        schedule_interval='0 0 * * *',
        catchup = False
        )

task_0 = BashOperator(
    task_id = "path",
    bash_command=f"echo {BASE_DIR} : ; pwd",
    dag=dag,
        )

task_1 = BashOperator(
    task_id="dump_db",
    bash_command=f'mysqldump -h localhost -u admin -p1234 kurrant_de > {BASE_DIR+"/db_dumps/"}"$(date +\%Y_\%m_\%d).sql"',
    dag=dag,
        )
task_2 = BashOperator(
    task_id="load_dump",
    bash_command=f'mysql -u admin -p1234 airflow_checker <  {BASE_DIR+"/db_dumps/"}"$(date +\%Y_\%m_\%d).sql"',
    dag=dag,
        )

task_0>>task_1 >> task_2
