"""
The deadline callback.  This works as written since the reference is the exercise, not the callback.

A SyncCallback is handed to the executor and runs like a task with top priority, which is why ``print``
works and why the output turns up in ordinary task logs.  An AsyncCallback would run on the triggerer
instead and log there.

Airflow passes a ``context`` kwarg describing the Dag run and the deadline.  Accept `**kwargs`` and read
``kwargs["context"]``, or declare a named ``context`` parameter; Airflow only passes what your callable
actually accepts.
"""

from __future__ import annotations


def log_missed_deadline(**kwargs) -> None:
    context = kwargs.get("context", {})
    dag_run = context.get("dag_run", {})
    deadline = context.get("deadline", {})

    print(f"🚨 DEADLINE MISSED: Dag '{dag_run.get('dag_id')}' did not finish by {deadline.get('deadline_time')}")
