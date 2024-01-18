import json
from airflow.hooks.base_hook import BaseHook
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator

#THIS DAG CAN HELP YOU TO EXPORT AIRFLOW 1.10 CONNECTIONS
#IN AIRFLOW >= 2 YOU CAN USE THE CLI AIRFLOW CONNECTIONS EXPORT

default_args = {
    'owner': 'vargas',
    'start_date': days_ago(0),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False
}

connections = [...] #all your connections name, you can get a list in UI or 'SELECT * FROM connection'

with DAG(dag_id=f'export_airflow_connections', schedule_interval=None, default_args=default_args,  is_paused_upon_creation=True, catchup=False) as dag:


    def decrypt_and_save_connections():
        connections_decrypted = {}

        for i in range(len(connections)):
            connection = BaseHook.get_connection(connections[i])

            conn_id = connection.conn_id
            conn_type = connection.conn_type  # Fix: Use correct attribute
            schema = connection.schema
            conn_password = connection.password
            conn_login = connection.login
            port = connection.port
            host = connection.host
            extra = connection.extra

            payload = {
                conn_id: {
                    "conn_type": conn_type,
                    "host": host,
                    "login": conn_login,
                    "password": conn_password,
                    "schema": schema,
                    "port": port,
                    "extra": extra
                }
            }
            connections_decrypted.update(payload)

        with open('/home/airflow/connections_airflow_delete_after_use.txt', 'w') as file:
            json.dump(connections_decrypted, file)

    decrypt_and_save_task = PythonOperator(
        task_id='decrypt_and_save_task',
        python_callable=decrypt_and_save_connections
       
    )

    decrypt_and_save_task
