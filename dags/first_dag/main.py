from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# Função que será executada pela DAG
def print_hello_world():
    print("Hello World")

# Definição da DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'hello_world_dag',
    default_args=default_args,
    description='A simple Hello World DAG',
    schedule_interval='0 9 * * *',
    start_date=days_ago(1),
    catchup=False,
)

# Definição da tarefa
hello_world_task = PythonOperator(
    task_id='print_hello_world_task',
    python_callable=print_hello_world,
    dag=dag,
)

hello_world_task
