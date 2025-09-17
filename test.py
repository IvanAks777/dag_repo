from datetime import datetime
from airflow.decorators import dag, task



@dag(dag_id='xcom_dag_v4_multi',start_date=datetime(2025,9,5), schedule='@daily', catchup=False)
def xcom_dag_v4_multi():

    @task
    def task_a(ti):
        val = 42
        ti.xcom_push(key='my_key', value=val)

    @task
    def task_c(ti):
        val = 43
        ti.xcom_push(key='my_key', value=val)

    @task
    def task_d(ti):
        val = {
            'a': 1,
            'b':2
        }
        ti.xcom_push(key='my_key', value=val)

    @task
    def task_b(ti):
        vals= ti.xcom_pull(task_ids=['task_a', 'task_c'] , key='my_key')
        print(*vals, sep='\n')
        vals2= ti.xcom_pull(task_ids='task_d' , key='my_key')
        print(vals2)

    [task_a(), task_c(), task_d()] >> task_b() # type: ignore


xcom_dag_v4_multi()