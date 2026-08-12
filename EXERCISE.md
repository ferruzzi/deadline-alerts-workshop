# The Exercise: CloseOfBusinessDeadline

Your Dag generates a report which the boss needs before they leave.  When is that? It isn't always "by 17:00
today".  Weekends, holidays, time zones, and tee-times all get a vote, and no built-in Deadline Reference knows 
your business calendar.

So let's teach Airflow yours.

By the end you will have a custom Reference that returns the next close of business, wired into a Dag with a 
negative interval so it warns you *before* the deadline rather than after.

## Setup

Copy the two directories into your Airflow home, or point Airflow at them:

```bash
cp plugins/* $AIRFLOW_HOME/plugins/
cp dags/* $AIRFLOW_HOME/dags/
```

Then **restart Airflow**.  Plugin registration happens at import, so a restart is needed after every change to 
a plugin file.  This will be the single most common reason a change you make in this workshop doesn't appear to 
do anything.

Set the two Variables which the finished version will need:

```bash
airflow variables set cob_config '{"hour": 17, "minute": 0}'
airflow variables set us_holidays '["2026-12-25", "2026-01-01", "2026-07-04"]'
```

## Checking Your Work

There is a checker in [`checker/`](checker/README.md) that runs your Reference with no Airflow involved:

```bash
python checker/check.py
```

It evaluates your Reference, shows the deadline Airflow would store, round-trips your serializers, and tells you
whether the plugin registration is in place.  Use it at every checkpoint below.  It is a much faster loop than
restart, trigger, read logs, and it catches the two mistakes that are otherwise completely silent: dropped fields
and missing registration.

With no arguments it checks `plugins/cob_reference.py`, which is the file you are editing, and it prints the path it
checked so you can see it picked the right one.  Run it from anywhere; it works out its own paths.  You can hand it
a different file if you want to look at a solution, and `checker/README.md` covers that.

If you could not get Airflow running at all, this is also the whole exercise; the file you write is the same
either way.

## Step 1: Subclass `BaseDeadlineReference`

Open `plugins/cob_reference.py`.  The class is already there and it needs one method, `_evaluate_with()`, 
which returns the timestamp everything else is measured from.

Airflow adds the interval to whatever you return here, and the sum is the deadline.

### Checkpoint

```bash
python checker/check.py
```

Expect it to tell you `_evaluate_with()` is not implemented, which is Step 2, and that the decorator is missing,
which is Step 3.  Worth running once so the output is familiar later.

## Step 2: Hardcode a close of business in the past

Two things matter for this step:
1. It must be **timezone-aware**  
2. For this step it must be **in the past**.  We want it to fire immediately so we can verify that it worked.

Set this to whatever past date you want.  If you can't think of one, here's the date/time of the moon landing:
```python
return datetime(1969, 7, 20, 20, 17, tzinfo=timezone.utc)
```

### Checkpoint

```bash
python checker/check.py
```

It should now print a reference timestamp and a deadline marked "already passed, fires immediately".  It will still
flag the missing registration, which is Step 3.

## Step 3: Register the Reference

Decorate the class:

```python
@deadline_reference()
class CloseOfBusinessDeadline(BaseDeadlineReference):
```

The parentheses matter on Airflow 3.3.0.  Used bare, the decorator rebinds your class to a function and fails 
later with a confusing error.  This is a bug which will be fixed in an upcoming release.

**Then register it in a plugin**, in the same file:

```python
from airflow.plugins_manager import AirflowPlugin


class CobPlugin(AirflowPlugin):
    name = "cob_plugin"
    deadline_references = [CloseOfBusinessDeadline]
```

Both are required, and they do different jobs.  The decorator makes the class available to your Dag file as 
`DeadlineReference.CloseOfBusinessDeadline`.  The plugin is what lets the **scheduler** find the class again 
when it deserializes your Dag.  If you skip the plugin then triggering the Dag fails with 
`DeadlineReferenceNotRegistered`, which takes the whole Dag run with it rather than just the deadline.

Restart Airflow and trigger `cob_deadline_demo`, then look for the 🚨 in the task logs.  You should have a firing 
deadline before you have any real logic. That is the point.

### Checkpoint

```bash
python checker/check.py
```

Everything should pass now: `All good: 1 reference(s) checked.`

**In Airflow:** this is the first point where the whole thing runs.  Restart, trigger `cob_deadline_demo`, and the
🚨 shows up in the task logs on the next scheduler heartbeat.  You have a working custom Deadline Reference before
you have written a single line of business logic.

`solutions/step3_hardcoded.py` is the matching known-good example if you want to compare.

## Step 4: Wire it up with a negative interval

Your Reference is working, but the Dag is not finished.  In `dags/cob_deadline_demo.py`, change the interval to a
negative timedelta:

```python
interval=timedelta(minutes=-30),
```

Intervals are added to the reference, so a negative one puts the deadline *before* it.  This is handy if you want 
warning before the set time, while you can still do something, rather than after we have already missed it.  A
message an hour after you close saying you missed the Close of business isn't particularly useful.  A positive 
interval, on the other hand, might be used to say "I need this within an hour after we open." 

The effect is academic while your Reference still returns a hardcoded date in the past, since the deadline was
already overdue and 30 minutes earlier is no different.  It starts mattering in Step 5, when the close of business
becomes a real time.

### Checkpoint

```bash
python checker/check.py
```

The checker reads the interval out of `dags/cob_deadline_demo.py`, so the `interval` and `deadline` lines now
reflect what you actually wrote.  Before this step it reports `0:30:00` and points out that a positive interval puts
the deadline *after* the reference; afterwards it shows `-1 day, 23:30:00`, which is how a `timedelta` prints minus
thirty minutes.

**In Airflow:** the deadline moves 30 minutes earlier than the close of business your Reference returns.  Step 7 is
where you watch that matter.

`solutions/cob_deadline_demo.py` is the finished Dag.

## Step 5: Read the time from a Variable

Hardcoding works, but shipping it means a code change every time you want the deadline moved.  What happens if the 
boss decides they are leaving at 4 every Wednesday from now on?  Let's read it from an Airflow Variable instead.

Make the class a `@dataclass` so it can take fields, and **give every field a default**.  Registration constructs 
your class with no arguments, so a field without a default breaks at import.

```python
@deadline_reference()
@dataclass
class CloseOfBusinessDeadline(BaseDeadlineReference):
    cob_variable_name: str = "cob_config"
    holidays_variable_name: str | None = "us_holidays"
```

Note the decorator order. `@dataclass` is applied first, so registration sees a finished dataclass.

Then read the Variables inside `_evaluate_with()`:

```python
config = Variable.get(self.cob_variable_name, default=DEFAULT_COB, deserialize_json=True)
```

`deserialize_json=True` parses the JSON for you, and `default=` means an unset Variable gets a documented fallback 
rather than a crash.

Now let's add the business-day logic: if it is past close of business today, or today is a weekend or holiday, roll 
forward to the next working day.

### Checkpoint

```bash
python checker/check.py
```

Your Reference should now compute a real close of business rather than a date in the past.  The checker will report
the fields it found and flag them as lost, which is Step 6.

**In Airflow:** it runs, and it looks correct.  That is the trap.  The demo Dag passes the same Variable names as
your field defaults, so the missing serializers change nothing you can observe.  Nothing warns you.  The bug only
surfaces the day somebody points the Reference at a different Variable and gets the default anyway.

`solutions/step5_variable.py` is the matching known-good example if you want to compare.

## Step 6: Add the serializers

Your Reference travels with the serialized Dag, and the scheduler rebuilds it from a dict.  The inherited 
`serialize_reference()` carries only the class name, which was fine in Step 3 but is now actively harmful since
your new fields get silently reset to their defaults on the way back.  So we need to teach Airflow how to handle 
your fields.

```python
def serialize_reference(self) -> dict[str, Any]:
    return {
        "reference_type": self.reference_name,
        "cob_variable_name": self.cob_variable_name,
        "holidays_variable_name": self.holidays_variable_name,
    }


@classmethod
def deserialize_reference(cls, reference_data: dict[str, Any]) -> CloseOfBusinessDeadline:
    return cls(
        cob_variable_name=reference_data["cob_variable_name"],
        holidays_variable_name=reference_data["holidays_variable_name"],
    )
```

### Checkpoint

```bash
python checker/check.py
```

Back to `All good`, and this time `fields survive` is listed.  That line is the point of the step: the checker sends
probe values through `serialize_reference()` and `deserialize_reference()` and reports any field that comes back
wrong.  Comparing the serialized dict alone would have passed at Step 5 too.

**In Airflow:** it runs, and nothing visibly changes from Step 5.  That is expected, and it is exactly why the
checker exists.

`solutions/cob_reference.py` is the finished Reference.

## Step 7: Test both directions

**A close of business in the past should fire immediately.**

```bash
airflow variables set cob_config '{"hour": 0, "minute": 1}'
airflow dags trigger cob_deadline_demo
```

The deadline is already overdue, so the callback runs on the next scheduler heartbeat.  Check the task logs for 🚨.

**A close of business in the future fires when it arrives.** 

Add about 33 minutes to the current time and replace the placeholders in the snippet below.  Since the interval
subtracts 30 minutes, that lands the deadline a few minutes from now.

```bash
airflow variables set cob_config '{"hour": <hour>, "minute": <minute>}'
airflow dags trigger cob_deadline_demo
```

This test needs the Dag to still be running when the deadline passes, which is why the demo Dag sleeps for 300 seconds.
**A Dag run that succeeds deletes any deadline it did not breach**, so a Dag that finishes in two seconds will never 
fire a future deadline no matter how carefully you set the Variable.

## Stretch goals

- **Holidays from a Variable.** Already scaffolded via `holidays_variable_name`.  What else belongs in config rather than code?
- **Half days.** Some businesses close at 13:00 on Fridays in summer.  Where does that live?
- **A real timezone.** This exercise uses local time deliberately, so it cannot fail on a machine without a timezone 
  database.  Take a timezone name as a third field and use `ZoneInfo(name)`, and note that it needs `tzdata` installed.

## Gotchas worth knowing

- **Restart after every plugin change.** Registration happens at import.
- **Define your Reference in the plugins folder, not in your Dag file.** Plugin discovery never scans the dags 
  folder, so a Reference defined alongside your Dag can never be registered.  It will parse and serialize fine, 
  then fail at Dag run creation.
- **`airflow dags trigger <dag_id>` with no `-l` leaves `logical_date` NULL.**  It does not matter for this exercise, 
  but a Reference built on `DAGRUN_LOGICAL_DATE` gets no deadline at all in that case. Pass `-l "$(date -Iseconds)"` 
  or trigger from the UI.
- **`Variable.get()` inside `_evaluate_with()` is safe here, but not on a scheduled Dag.** The scheduler creates
  scheduled runs inside a `prohibit_commit` guard, and `Variable.get` opens its own session and commits, which
  drops the deadline silently.  This exercise triggers manually, so the run is created in the API server and the
  call is fine.  [#68917](https://github.com/apache/airflow/pull/68917) has the fix and the reasoning behind it.

  Until that fix gets released, Variables will work on scheduled Dags if you pass the existing session to the
  metadata store:

  ```python
  from airflow.secrets.metastore import MetastoreBackend

  raw = MetastoreBackend().get_variable("cob_config", session=session)
  ```

  That joins the existing transaction instead of opening a second one.  Note that it reads only the metadata
  database, so `AIRFLOW_VAR_*` env vars and other secrets backends are skipped.
- **`VariableInterval` requires a positive number of seconds.** Variable-backed *intervals* cannot be negative, 
  even though hardcoded negative timedeltas are supported and documented.  If you want "alert before" plus 
  Variable-backed config, put the configuration in the Reference, which is what this exercise does.  This is a bug 
  which will be fixed in a coming release.
