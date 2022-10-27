import base64

from airflow import DAG,XComArg
from airflow.operators.python import PythonOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.bash_operator import BashOperator
from airflow.hooks.base_hook import BaseHook
from airflow.decorators import task
from airflow.utils.dates import days_ago
from datetime import datetime,timedelta



default_args = {
    'owner': 'guilherme',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

ENV = 'dev' #CHANGE

def brands_to_create_view(**kwargs):
    ti = kwargs['ti'].xcom_pull(task_ids='databases_without_logs_insights_view', key='return_value')
    decoded_databases = ((base64.b64decode(ti).decode('utf-8')))

    return decoded_databases.split("\n")      

with DAG("final_logs_insights", schedule_interval=None, start_date= datetime(2022, 10, 27)) as dag:

    databases_without_logs_insights_view = SSHOperator(
        task_id='databases_without_logs_insights_view',
        ssh_conn_id=f"clickhouse-{ENV}",
        command='''clickhouse-client -q "SELECT DISTINCT database FROM system.tables WHERE database NOT IN (SELECT DISTINCT database FROM system.tables WHERE name = 'logs_insights') "''',
        do_xcom_push=True
        )


    databases_list = PythonOperator(
        task_id='databases_list',
        python_callable=brands_to_create_view
    )
    

    conn_from = BaseHook.get_connection(f'clickhouse-{ENV}')
    sftp_host = conn_from.host
    sftp_user = conn_from.login

    @task
    def loop(brand):
        if brand:
            vars = {"database": f"{brand}" ,"profile": "general"}
            return f'''ssh {sftp_user}@{sftp_host} 'docker exec -t dbt-dbt_server-1 dbt run -s models/marts/logs_insights/logs_insights.sql --vars "{vars}" --project-dir /usr/app/dbt/project/'  '''
        else:
            return f'echo everything is okay'
            
    commands = loop.partial().expand(brand=XComArg(databases_list))


    create_views_on_missing_databases = BashOperator.partial(
        task_id='create_views_on_missing_databases').expand(bash_command=commands)


    run_dbt_final_logs_insights = SSHOperator(
        task_id='run_dbt_final_logs_insights',
        ssh_conn_id=f"clickhouse-{ENV}",
        command='''docker exec -t dbt-dbt_server-1 dbt run -s models/marts/logs_insights/final_logs_insights.sql --vars '{"database":"default","profile":"final-insights"}' --project-dir /usr/app/dbt/project/'''
    )


databases_without_logs_insights_view >> databases_list >>commands>> create_views_on_missing_databases >> run_dbt_final_logs_insights
