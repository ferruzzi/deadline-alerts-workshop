"""
Solution: the demo Dag, wired up.

Note the negative interval.  Airflow adds the interval to whatever the reference returns, so
``timedelta(minutes=-30)`` puts the deadline half an hour *before* close of business.
"""

from __future__ import annotations

from datetime import timedelta

from cob_reference import CloseOfBusinessDeadline
from deadline_callbacks import log_missed_deadline

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, DeadlineAlert, SyncCallback

with DAG(
    dag_id="cob_deadline_demo",
    schedule=None,
    deadline=DeadlineAlert(
        # Instantiating directly rather than using DeadlineReference.CloseOfBusinessDeadline lets you pass fields.
        reference=CloseOfBusinessDeadline(
            cob_variable_name="cob_config",
            holidays_variable_name="us_holidays",
        ),
        interval=timedelta(minutes=-30),
        callback=SyncCallback(log_missed_deadline),
    ),
):
    BashOperator(task_id="long_running_task", bash_command="sleep 300")
