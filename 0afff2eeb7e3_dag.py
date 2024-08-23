from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from datetime import datetime, timedelta 

#Define params for Submit Run Operator
notebook_task = {
    'notebook_path': '/Users/seamus.holland@icloud.com/data_pipeline',
}

default_args = {
    'owner': 'swhh',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(dag_id='0afff2eeb7e3_dag',
    start_date=datetime(2024, 8, 15),
    schedule_interval='@daily',
    catchup=False,
    default_args=default_args
    ) as dag:
    opr_submit_run = DatabricksSubmitRunOperator(
        task_id='submit_run',
        databricks_conn_id='databricks_default',
        existing_cluster_id='1108-162752-8okw8dgg',
        notebook_task=notebook_task
    )
    opr_submit_run