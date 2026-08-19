# The Checker (No Airflow Required)

`check.py` validates a custom Deadline Reference without Airflow installed.

**Use it even if you have Airflow.** It runs in about a second, where the Airflow loop is restart, trigger, wait
for a heartbeat, read the logs. It also catches two mistakes Airflow reports silently: fields dropped by missing
serializers, and a Reference that was decorated but never registered in a plugin.

If you could not get Airflow running at all, this is your whole exercise. Writing a custom Deadline Reference is
mostly business-day arithmetic, timezone handling, and a small serialization contract.  None of that needs a
scheduler.

## Requirements

Python 3.10 or newer.  Nothing else.

## Usage

Follow [EXERCISE.md](../EXERCISE.md) as written, editing the same `plugins/cob_reference.py`.  At each checkpoint,
instead of restarting Airflow and triggering a Dag, run:

```bash
python checker/check.py plugins/cob_reference.py
```

Either path works, from the repo root or from this directory:

```bash
cd checker && python check.py                            # the file you are editing
python check.py ../solutions/cob_reference_step3_registered.py           # or check a solution
python check.py ../solutions/cob_reference_step6_final.py
```

With no arguments it looks for `$AIRFLOW_HOME/plugins/cob_reference.py` first, since Setup has you copy
the files there and edit the copies, and falls back to `plugins/cob_reference.py` in the repo for anyone
working without Airflow.  The interval is resolved the same way, preferring
`$AIRFLOW_HOME/dags/cob_deadline_demo_dag.py`.  The path it used is always printed.

A second path points it at a different Dag, which is useful in Part 4 when you are building your own:

```bash
python check.py ../plugins/cob_reference.py ../dags/my_own_dag.py
```

Before you have written anything, it tells you what is missing:

```
=== CloseOfBusinessDeadline ===

2 thing(s) to fix:

  - CloseOfBusinessDeadline is missing the @deadline_reference() decorator.
  - CloseOfBusinessDeadline._evaluate_with() is not implemented yet.  That is Step 2: ...
```

When you are done it looks like this:

```
=== CloseOfBusinessDeadline ===
  reference          2026-09-01 17:00:00-05:00
  interval           -1 day, 23:30:00  (from cob_deadline_demo_dag.py)
  deadline           2026-09-01 16:30:00-05:00  (in the future)
  serialized         {'reference_type': 'CloseOfBusinessDeadline', 'cob_variable_name': 'cob_config', ...}
  round-trip         ok
  fields survive     ok (cob_variable_name)
  plugin             registered

All good: 1 reference(s) checked.
```

`-1 day, 23:30:00` is how a `timedelta` prints minus thirty minutes.

## What It Checks

1. Your Reference can be constructed with no arguments, which registration requires.
2. `_evaluate_with()` returns a timezone-aware datetime.
3. `serialize_reference()` and `deserialize_reference()` round-trip, **and** your field values
   survive the trip.  A class with fields and the inherited serializers round-trips cleanly while
   silently resetting every field to its default, so this check uses probe values to catch it.
4. Your Reference is listed in an `AirflowPlugin.deadline_references`.

Number 4 is the one that costs people an afternoon on a real install: decorating the class is not
enough, and an unregistered Reference fails when the Dag run is created.

## A Word of Warning

This is a teaching aid, not production code, and it is deliberately brittle.

The interval lives in the Dag rather than the Reference, so the checker reads
`dags/cob_deadline_demo_dag.py` and looks for exactly the shape this exercise uses: a `DeadlineAlert(...)`
call with `interval=timedelta(...)`.  Rename things, lift the interval into a constant, or compute it,
and it falls back to an illustrative `-30 minutes` and says so on the `interval` line.  It parses the
Dag rather than importing it, which is why it needs no stubs for operators or callbacks.

Everything else it reports comes from your Reference file, so the exit code only ever reflects that file.

## Variables

`Variable.get()` reads from `variables.json` in this directory instead of the Airflow metadata
database.  Edit that file to change the close-of-business time, exactly as you would with
`airflow variables set`.

## How It Works

Before importing your file, `check.py` registers stand-in `airflow.*` modules in `sys.modules`.
Your imports resolve against those stubs, so the file you write here is the *same file* that runs
on a real Airflow install; nothing needs changing when you move it.

The stubs mirror the real behavior where it matters, including requiring parentheses on
`@deadline_reference()`, which Airflow 3.3.0 also requires.  The checker is deliberately no more
permissive than the real thing.

## What You Miss

You will not see a deadline actually fire, since that needs a running scheduler and executor, but 
it will be demonstrated live during the session.

## Afterwards

Everything you write here transfers unchanged to a real Airflow install.  Copy `plugins/` into your 
plugins directory, copy `dags/` into your dags directory, restart, and follow the main
[README](../README.md).
