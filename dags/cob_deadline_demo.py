"""
The demo Dag.  Copy this ``dags/`` directory into your Airflow dags folder.

The sleeping task is not decoration.  When a Dag run succeeds, Airflow prunes any Deadline it did not breach,
so a Dag that finishes in two seconds deletes its own Deadline before a future close of business can ever be
missed.  Five minutes of sleep is what makes "set it three minutes out and watch it fire" possible in step 7.
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
        reference=CloseOfBusinessDeadline(),
        # TODO (step 4): alert 30 minutes BEFORE close of business rather than after it.
        interval=timedelta(minutes=30),
        callback=SyncCallback(log_missed_deadline),
    ),
):
    BashOperator(task_id="long_running_task", bash_command="sleep 300")
