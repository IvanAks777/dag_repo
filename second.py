from airflow.decorators import dag

from airflow.operators.weekday import BranchDayOfWeekOperator

from airflow.operators.empty import EmptyOperator

from airflow.utils.weekday import WeekDay

import pendulum


@dag(schedule_interval=None, start_date=pendulum.datetime(2022, 12, 1), catchup=False)
def branchdayofweek():

    empty_task_12 = EmptyOperator(task_id="thursday_or_sunday")

    empty_task_22 = EmptyOperator(task_id="not_thursday_or_sunday")

    empty_task_32 = EmptyOperator(task_id="one_more_task")

    branch_weekend = BranchDayOfWeekOperator(
        task_id="make_day_choice",
        follow_task_ids_if_true=["thursday_or_sunday", 'one_more_task'],
        follow_task_ids_if_false="not_thursday_or_sunday",
        week_day={WeekDay.TUESDAY, WeekDay.SUNDAY}
        )

    branch_weekend >> [empty_task_12, empty_task_22, empty_task_32]


dag = branchdayofweek()
