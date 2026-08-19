"""
Solution: the demo Dag, wired up.

Note the negative interval.  Airflow adds the interval to whatever the reference returns, so
``timedelta(minutes=-30)`` puts the deadline half an hour *before* close of business.
"""

from __future__ import annotations

from datetime import timedelta

from deadline_callbacks import log_missed_deadline

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, DeadlineAlert, DeadlineReference, SyncCallback

with DAG(
    dag_id="cob_deadline_demo",
    schedule=None,
    deadline=DeadlineAlert(
        # Since we register the DeadlineReference, it is in the DeadlineReference namespace, alongside the built-ins.
        # No parentheses needed since registration already instantiated it for you.  Unlike a parameterized built-in
        # such as DeadlineReference.FIXED_DATETIME(dt), a custom reference cannot take arguments through the
        # namespace.  To pass field values, import the class and instantiate it yourself:
        #     CloseOfBusinessDeadline(cob_variable_name="other_config")
        reference=DeadlineReference.CloseOfBusinessDeadline,
        interval=timedelta(minutes=-30),
        callback=SyncCallback(log_missed_deadline),
    ),
):
    BashOperator(task_id="long_running_task", bash_command="sleep 300")
