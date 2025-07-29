from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime

# Define default_args and DAG
default_args = {
    'start_date': datetime(2024, 1, 1),
}


def pull_date_from_conf(**context):
    conf = context['dag_run'].conf or {}
    return conf.get("date", "1970-01-01")

"""
def local_run(**context):
    conf = context['dag_run'].conf or {}
    date = conf.get("date", "1970-01-01")

    import requests
    
    response = requests.get('http://lambda:8080/2015-03-31/functions/function/invocations')

    status_code = response['StatusCode']
    if status_code != 200:
        raise Exception(f"Lambda failed with status code {status_code}")

    response_payload = response['Payload'].read().decode('utf-8')
    print("Lambda response:", response_payload)
    #print(response['Payload'].read().decode())

    ok_args = ""...

"""




with DAG(
    dag_id='p3_local_pipeline',
    default_args=default_args,
    description='P3 Pipeline for local execution',
    schedule_interval=None,  
    catchup=False,
    tags=['P3'],
) as dag:
    
    get_date_task = PythonOperator(
        task_id="get_datepipeline",
        python_callable=pull_date_from_conf,
        provide_context=True
    )
    
    extract = BashOperator(
        task_id='extract',
        bash_command=(
            'curl "http://lambda:8080/2015-03-31/functions/function/invocations" '
            '-d \'{"step":"extract","date":"{{ ti.xcom_pull(task_ids="get_datepipeline") }}","env":"dev"}\''
        )    
    )

    load = BashOperator(
        task_id='load',
        bash_command=(
            'curl "http://lambda:8080/2015-03-31/functions/function/invocations" '
            '-d \'{"step":"load","date":"{{ ti.xcom_pull(task_ids="get_datepipeline") }}","env":"dev"}\''
        )      
    )

    transformation = BashOperator(
        task_id='transformation',
        bash_command=(
            'curl "http://lambda:8080/2015-03-31/functions/function/invocations" '
            '-d \'{"step":"transformation","date":"{{ ti.xcom_pull(task_ids="get_datepipeline") }}","env":"dev"}\''
        )  
    )

    get_date_task >> extract >> load >> transformation
