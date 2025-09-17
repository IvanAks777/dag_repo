from datetime import datetime
from airflow.decorators import dag, task



@dag(dag_id='xcom_dag_v3',start_date=datetime(2025,9,5), schedule='@daily', catchup=False)
def xcom_dag_v3():

    @task
    def task_a(ti):
        val = 42
        ti.xcom_push(key='my_key', value=val)

    @task
    def task_b(ti):
        val = ti.xcom_pull(task_ids='task_a', key='my_key')
        print(val)

    task_a() >> task_b() # type: ignore


xcom_dag_v3()

