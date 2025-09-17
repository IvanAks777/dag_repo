from datetime import datetime
from airflow.decorators import dag, task



@dag(dag_id='xcom_dag_v2',start_date=datetime(2025,9,5), schedule='@daily')
def xcom_dag_v2():

    @task
    def task_a():
        val = 42
        return val


    @task
    def task_b(val: int):
        print(val)

    task_b(task_a()) # type: ignore


xcom_dag_v2()