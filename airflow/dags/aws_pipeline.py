from airflow import DAG
from airflow.operators.python import PythonOperator
#from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime, timedelta
import boto3
import json
from pathlib import Path
from dotenv import load_dotenv

import os

dotenv_path = Path('.env/.venv')
load_dotenv(dotenv_path=dotenv_path)

SUBNETS = os.getenv('SUBNETS')
SECURITYGROUPS = os.getenv('SECURITYGROUPS')


os.environ['AWS_SHARED_CREDENTIALS_FILE'] = '/opt/bitnami/airflow/.aws/credentials'


def invoke_lambda_1(**context):
    datepipeline = context['dag_run'].conf.get("date", "1970-01-01")
    #boto3.set_stream_logger('botocore', level='DEBUG')
    client = boto3.client('lambda', region_name='us-east-1')

    payload = {"step":"extract","date":datepipeline,"env":"prd"}

    response = client.invoke(
        FunctionName='lambda-lastfm',
        InvocationType='RequestResponse',
        Payload=json.dumps(payload),
    )

    status_code = response['StatusCode']
    if status_code != 200:
        raise Exception(f"Lambda failed with status code {status_code}")

    response_payload = response['Payload'].read().decode('utf-8')
    print("Lambda response:", response_payload)
    #print(response['Payload'].read().decode())


def invoke_lambda_2(**context):
    datepipeline = context['dag_run'].conf.get("date", "1970-01-01")
    client = boto3.client('lambda', region_name='us-east-1')

    payload = {"step":"load","date":datepipeline,"env":"prd"}

    response = client.invoke(
        FunctionName='lambda-lastfm',
        InvocationType='RequestResponse',
        Payload=json.dumps(payload),
    )

    status_code = response['StatusCode']
    if status_code != 200:
        raise Exception(f"Lambda failed with status code {status_code}")

    response_payload = response['Payload'].read().decode('utf-8')
    print("Lambda response:", response_payload)
    #print(response['Payload'].read().decode())

# ----- Fargate Task -----
def run_fargate_task(**context):
    datepipeline = context['dag_run'].conf.get("date", "1970-01-01")
    client = boto3.client('ecs', region_name='us-east-1')
    response = client.run_task(
        cluster='lastfm-elt-cluster',
        launchType='FARGATE',
        taskDefinition='lastfm-transformation:6', # HARD CODED VERSION
        count=1,
        networkConfiguration={
            'awsvpcConfiguration': {
                'subnets': ['subnet-0d2ad026359acb35b','subnet-0c60ec54e99ee2e73','subnet-0da8f7bfe1b0fb041','subnet-0841975439c6e98f9','subnet-0bc424d2f30284780','subnet-014990a9c2a21283b'],
                'securityGroups': ['sg-0864716ae56f03d2c'],
                'assignPublicIp': 'ENABLED'
            }
        },
        overrides={
            'containerOverrides': [
                {
                'name': 'lastfm-elt',
                'environment': [
                    {'name': 'DATE', 'value': datepipeline},
                    {'name': 'ENV',  'value': 'prd'},
                    {'name': 'STEP', 'value': 'transformation'}
                ]
                }
            ]
        }
    )
    task_arn = response['tasks'][0]['taskArn']
    print(f"Started Fargate task: {task_arn}")

    # Wait for task to complete
    waiter = client.get_waiter('tasks_stopped')
    waiter.wait(
        cluster='lastfm-elt-cluster',
        tasks=[task_arn],
        WaiterConfig={
            'Delay': 15,
            'MaxAttempts': 40  # ~10 minutes
        }
    )

    print("Fargate task completed.")

# ----- Snowflake Query -----
snowflake_sql = """
SELECT CURRENT_DATE;
"""

# ----- DAG Definition -----
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='p3_aws_pipeline',
    default_args=default_args,
    description='P3 Pipeline for aws execution',
    schedule_interval=None,
    catchup=False,
    tags=['P3'],
) as dag:

    extract = PythonOperator(
        task_id='extract',
        python_callable=invoke_lambda_1,
        provide_context=True
    )

    load = PythonOperator(
        task_id='load',
        python_callable=invoke_lambda_2,
        provide_context=True    
    )

    transformation = PythonOperator(
        task_id='transformation',
        python_callable=run_fargate_task,
        provide_context=True
    )
    #snowflake_task = SnowflakeOperator(
    #    task_id='run_snowflake_query',
    #    sql=snowflake_sql,
    #    snowflake_conn_id='snowflake_default',
    #)

    # Task dependencies
    extract >> load >> transformation
