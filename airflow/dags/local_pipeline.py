from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

# Define default_args and DAG
default_args = {
    'start_date': datetime(2024, 1, 1),
}

"""
{
  "my_param": "hello_from_ui",
  "run_type": "ui"
}

curl "http://localhost:8080/2015-03-31/functions/function/invocations" -d '{"step":"transformation","date":"2024-11-15","env":"dev"}'

"""

def print_config(**context):
    conf = context['dag_run'].conf
    print(f"Config passed to DAG: {conf}")
    print("My param:", conf.get("my_param", "not provided"))




with DAG(
    dag_id='local_pipeline',
    default_args=default_args,
    description='A simple DAG with three BashOperator tasks',
    schedule_interval=None,  # Manual trigger
    catchup=False,
    tags=['example'],
) as dag:

    # Task 1: Print date
    extract = BashOperator(
        task_id='extract',
        bash_command='curl "http://lambda:8080/2015-03-31/functions/function/invocations" -d \'{"step":"extract","date":"2024-11-15","env":"dev"}\''
    )

    # Task 2: Sleep for 5 seconds
    load = BashOperator(
        task_id='load',
        bash_command='curl "http://lambda:8080/2015-03-31/functions/function/invocations" -d \'{"step":"load","date":"2024-11-15","env":"dev"}\''
    )

    # Task 3: Echo a message
    transformation = BashOperator(
        task_id='transformation',
        bash_command='curl "http://lambda:8080/2015-03-31/functions/function/invocations" -d \'{"step":"transformation","date":"2024-11-15","env":"dev"}\''

    )


    # Set task dependencies
    extract >> load >> transformation
