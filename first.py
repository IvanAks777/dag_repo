from datetime import datetime
from airflow.decorators import dag, task



@dag(dag_id='xcom_dag',start_date=datetime(2025,9,5), schedule='@daily')
def xcom_dag():

    @task
    def task_a(**context):
        val = 42
        context['ti'].xcom_push(key='my_key', value=val)



    @task
    def task_b(**context):
        val = context['ti'].xcom_pull(task_ids='task_a', key='my_key')
        print(val)

    task_a() >> task_b() # type: ignore


xcom_dag()
