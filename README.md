# Advanced Deadline Alerts: Airflow Summit 2026 Workshop

Materials for the **Advanced Deadline Alerts** hands-on workshop at Airflow Summit 2026 (1 September 2026).

In this workshop you will build a custom Deadline Reference from scratch: a `CloseOfBusinessDeadline` that 
knows your business calendar, skips weekends, and warns you *before* the deadline rather than after.

**The walkthrough is in [EXERCISE.md](EXERCISE.md).**

If something appears to do nothing, check [TROUBLESHOOTING.md](TROUBLESHOOTING.md) before debugging your own
code.  Several things in Airflow 3.3.0 fail silently, and that file lists every one we know of, with the fix.

## Before You Arrive

**Please set this up before the session.**  Conference wifi will not be able to support 25 people downloading 
Airflow at once, and we only have two hours.

### Requirements

- **Apache Airflow 3.3.0 or newer**. Any installation method is fine: `pip` in a virtualenv, Docker, 
  `airflow standalone`, an existing dev environment, or Breeze if that is already your workflow.
- Ability to **add a file to your plugins directory** and restart Airflow.
- Ability to **set an Airflow Variable** (UI or CLI).
- Ability to **watch your scheduler's console output**, which is where the deadline callback output appears.
  (It is also written to `$AIRFLOW_HOME/logs/executor_callbacks/`.  `EXERCISE.md` has a table of where to find 
  it for each way of running Airflow, plus a `grep` one-liner that works without console access at all.)
- **Python 3.10 or newer**.  If Airflow runs you already have this.  Listed separately because the checker 
  needs only Python.

### Verify Your Setup

Run this against the same Python environment as your Airflow install:

```bash
python -c "
import airflow
from airflow.plugins_manager import AirflowPlugin
from airflow.sdk import DeadlineAlert, DeadlineReference, SyncCallback
from airflow.sdk.definitions.deadline import BaseDeadlineReference, deadline_reference
print('ready, airflow', airflow.__version__)
"
```

If it prints `ready` and a version of 3.3.0 or newer, you are set.  If it raises `ImportError` or
`ModuleNotFoundError`, your Airflow is either too old or not installed in that environment; the second is the more
common surprise, so check the version it prints even when the imports succeed.

Also confirm the version itself:

```bash
airflow version
```

### Get the Materials

Clone this repo ahead of time as well.  It is only a few kilobytes, so it is not the bandwidth problem the 
Airflow install is, but having it already on disk is one less thing to do in the room:

```bash
git clone https://github.com/ferruzzi/deadline-alerts-workshop.git
```

Exercise and solution files may still get small fixes, so **run `git pull` the morning of the session** 
to pick up the latest.

### Triggering the Demo Dag

Any of these work, and the exercise does not care which you use:

```bash
airflow dags trigger cob_deadline_demo                        # fine for this exercise
airflow dags trigger cob_deadline_demo -l "$(date -Iseconds)" # safest in general
```

The bare form leaves the run's `logical_date` unset.  That is harmless here, because
`CloseOfBusinessDeadline` computes its own time, but a Reference built on
`DeadlineReference.DAGRUN_LOGICAL_DATE` gets no deadline at all when it is NULL, and says nothing
about it.  Worth knowing before you go building your own in the free-form session.  Triggering from
the UI sets it for you.

## Didn't Get Set Up? You Can Still Participate

Two fallbacks, no preparation required:

1. **Pair up.**  One working laptop per pair is plenty, and talking through the logic with someone else is 
   arguably the better way to learn it.  We will try to pair people off at the start of the session.
2. **The checker on its own.**  The important part of this exercise, the business-day logic and the serialization 
   contract, does not need a running Airflow at all.  See [`checker/README.md`](checker/README.md) for a standalone 
   checker that needs only Python 3.10+ and no Airflow install.  You lose the option to see your work run
   in the Airflow UI, but it can still be validated.

**Everyone should use the checker, not just people without Airflow.**  `checker/check.py` validates your
Reference in about a second, where the Airflow loop is restart, trigger, wait for a heartbeat, read the logs.  It
also catches two mistakes that Airflow reports silently: fields dropped by missing serializers, and a Reference
that was decorated but never registered in a plugin.

```bash
python checker/check.py
```


## Repo Map

```
EXERCISE.md                           the step-by-step walkthrough; start here in the session
TROUBLESHOOTING.md                    known 3.3.0 sharp edges, symptom first; check here when something is silent
plugins/                              copy into $AIRFLOW_HOME/plugins/
  cob_reference.py                    your working file, with TODOs (plus a provided get_variable helper)
  deadline_callbacks.py               given to you, complete
dags/                                 copy into $AIRFLOW_HOME/dags/
  cob_deadline_demo_dag.py            the demo Dag, one TODO
solutions/                            checkpoints to compare against when you are stuck
  cob_reference_step3_registered.py   after Step 3: registered and firing, no business logic yet
  cob_reference_step5_variable.py     after Step 5: Variable-backed, serializers still to come
  cob_reference_step6_final.py        after Step 6: the finished Reference
  cob_deadline_demo_dag.py            the finished Dag
checker/
  check.py                            validates your Reference; also the whole exercise if you have no Airflow
  variables.json                      stands in for Airflow Variables when you have no Airflow
```

`cob_reference_step5_variable.py` is deliberately incomplete and the checker will say so.  That is the point of it: it is what
Step 6 exists to fix.


## Useful Links

- [Deadline Alerts documentation](https://airflow.apache.org/docs/apache-airflow/stable/howto/deadline-alerts.html)
- [Airflow Summit 2026](https://airflowsummit.org/)

## Questions

Find me at the Summit, or `@ferruzzi` on the [Apache Airflow Slack](https://apache-airflow-slack.herokuapp.com/).

## License

Apache License 2.0. See [LICENSE](LICENSE).
