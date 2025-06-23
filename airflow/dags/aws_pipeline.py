from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime, timedelta
import boto3

"""
Broken DAG: [/opt/bitnami/airflow/dags/lambda_lastfm.py]
Traceback (most recent call last):
  File "<frozen importlib._bootstrap>", line 488, in _call_with_frames_removed
  File "/opt/bitnami/airflow/dags/lambda_lastfm.py", line 3, in <module>
    from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
ModuleNotFoundError: No module named 'airflow.providers.snowflake'
"""

# ----- Lambda Functions -----
def invoke_lambda_1():
    client = boto3.client('lambda')
    response = client.invoke(
        FunctionName='lambda-function-1',
        InvocationType='RequestResponse'
    )
    print(response['Payload'].read().decode())

def invoke_lambda_2():
    client = boto3.client('lambda')
    response = client.invoke(
        FunctionName='lambda-function-2',
        InvocationType='RequestResponse'
    )
    print(response['Payload'].read().decode())

# ----- Fargate Task -----
def run_fargate_task():
    client = boto3.client('ecs')
    response = client.run_task(
        cluster='your-cluster-name',
        launchType='FARGATE',
        taskDefinition='your-task-def-name',
        networkConfiguration={
            'awsvpcConfiguration': {
                'subnets': ['subnet-xxxxxx'],
                'assignPublicIp': 'ENABLED'
            }
        }
    )
    print(response)

# ----- Snowflake Query -----
snowflake_sql = """
SELECT CURRENT_DATE;
"""

# ----- DAG Definition -----
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='lambda_fargate_snowflake_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['aws', 'snowflake'],
) as dag:

    lambda_task_1 = PythonOperator(
        task_id='invoke_lambda_1',
        python_callable=invoke_lambda_1
    )

    lambda_task_2 = PythonOperator(
        task_id='invoke_lambda_2',
        python_callable=invoke_lambda_2
    )

    fargate_task = PythonOperator(
        task_id='run_fargate_task',
        python_callable=run_fargate_task
    )

    snowflake_task = SnowflakeOperator(
        task_id='run_snowflake_query',
        sql=snowflake_sql,
        snowflake_conn_id='snowflake_default',
    )

    # Task dependencies
    lambda_task_1 >> lambda_task_2 >> fargate_task >> snowflake_task
