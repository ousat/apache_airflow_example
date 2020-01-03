
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator


def task_one():
    print("a phrase")

dag_args = {
    'owner': 'user',
    'depends_on_past': False,
    'start_date': datetime.now(),
    'email': ['satish.kumar@marketale.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 4,
    'retry_delay': timedelta(minutes=2),
    'schedule_interval': '@daily'
}

dag = DAG('AFDAG', description='test DAG', catchup=False, default_args=dag_args)


first_task = BashOperator(task_id='echo_hello', bash_command='echo "Hello"', dag=dag)
sleep = BashOperator(task_id='sleep', bash_command='sleep 5', dag=dag)
second_task = PythonOperator(task_id='run_function', python_callable=task_one, dag=dag)


first_task >> sleep >> second_task
