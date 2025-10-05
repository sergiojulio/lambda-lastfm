from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator

DATABRICKS_CONN_ID = 'databricks_default'

notebook_task_config = {
    "existing_cluster_id": "a1004-185319-4eog7244-v2n",  # put your real cluster ID
    "notebook_task": {
        "notebook_path": "/Workspace/Users/sergiojulio@gmail.com/notebook.ipynb"
    }
}

with DAG(
    dag_id="datbricks",
    start_date=days_ago(1),
    schedule_interval="@once",
    catchup=False,
) as dag:

    run_notebook = DatabricksSubmitRunOperator(
        task_id="run_my_databricks_notebook",
        databricks_conn_id=DATABRICKS_CONN_ID,
        json=notebook_task_config,
    )

    run_notebook
