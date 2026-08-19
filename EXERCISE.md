# The Exercise: CloseOfBusinessDeadline

Your Dag generates a report which the boss needs before they leave.  When is that? It isn't always "by 17:00
today".  Weekends, holidays, time zones, and tee-times all get a vote, and no built-in Deadline Reference knows 
your business calendar.

So let's teach Airflow yours.

By the end you will have a custom Reference that returns the next close of business, wired into a Dag with a 
negative interval so it warns you *before* the deadline rather than after.

## Setup

> [!NOTE]
> **No working Airflow?** Skip this whole section. Edit `plugins/cob_reference.py` right here in the repo,
> and instead of setting Airflow Variables, edit `checker/variables.json`. Everything from Step 1 onwards
> works exactly the same; you will just use the checker rather than triggering a Dag. See
> [`checker/README.md`](checker/README.md).

Copy the two directories into your Airflow home:

```bash
cp plugins/*.py $AIRFLOW_HOME/plugins/
cp dags/*.py $AIRFLOW_HOME/dags/
```

> [!IMPORTANT]
> From here on, **edit the copies in `$AIRFLOW_HOME`, not the files in this repo.** Those are the ones
> Airflow loads; edits to the repo copies have no effect. The checker prefers the `$AIRFLOW_HOME` copies
> too and prints the path it used, so you can always confirm which file it looked at.

Then **restart Airflow**.  Plugin registration happens at import, so a restart is needed after every change to 
a plugin file.  This will be the single most common reason a change you make in this workshop doesn't appear to 
do anything.

Set the Variable which the finished version will need:

```bash
airflow variables set cob_config '{"hour": 17, "minute": 0}'
```

Finally, **unpause the Dag**:

```bash
airflow dags unpause cob_deadline_demo
```

New Dags are paused when they first appear (`core.dags_are_paused_at_creation` defaults to `True`), and the toggle
is at the top left of the Dag page in the UI if you prefer clicking.  Triggering a paused Dag looks like it worked:
you get a Dag run, it sits in `queued`, and no task ever starts.  Nothing in the UI tells you why.  This is the
second most common reason something in this workshop appears to do nothing, right behind forgetting to restart
after editing a plugin.

## Checking Your Work

There is a checker in [`checker/`](checker/README.md) that runs your Reference with no Airflow involved:

```bash
python checker/check.py
```

It evaluates your Reference, shows the deadline Airflow would store, round-trips your serializers, and tells you
whether the plugin registration is in place.  Use it at every checkpoint below.  It is a much faster loop than
restart, trigger, read logs, and it catches the two mistakes that are otherwise completely silent: dropped fields
and missing registration.

With no arguments it checks the `cob_reference.py` you are editing.  If you copied the files into
`$AIRFLOW_HOME` as Setup says, it checks that copy, since that is the one Airflow loads; otherwise it falls
back to the one in this repo.  Either way it prints the path it checked, so you can confirm it picked the
right file.  Run it from anywhere; it works out its own paths.  You can hand it a different file if you want
to look at a solution, and `checker/README.md` covers that.

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

**Then register it in a plugin**, in the same file (`AirflowPlugin` is already imported for you):

```python
class CobPlugin(AirflowPlugin):
    name = "cob_plugin"
    deadline_references = [CloseOfBusinessDeadline]
```

Both are required, and they do different jobs.  The decorator makes the class available to your Dag file as 
`DeadlineReference.CloseOfBusinessDeadline`.  The plugin is what lets the **scheduler** find the class again 
when it deserializes your Dag.  If you skip the plugin then triggering the Dag fails with 
`DeadlineReferenceNotRegistered`, which takes the whole Dag run with it rather than just the deadline.

Restart Airflow and trigger `cob_deadline_demo` (unpaused, per Setup), then look for `FINDME` in your scheduler's 
console output (or grep the log file).  You should have a firing deadline before you have any real logic.

### Checkpoint

```bash
python checker/check.py
```

Everything should pass now: `All good: 1 reference(s) checked.`

**In Airflow:** this is the first point where the whole thing runs.  Restart, trigger `cob_deadline_demo`, and
`**FINDME**` shows up in your scheduler's console on the next scheduler heartbeat.  You have a working custom
Deadline Reference before you have written a single line of business logic.

> [!TIP]
> **Where the callback output goes.** A deadline callback is not a task instance, so its output is *not* in the
> task log and the UI has no page for it.  Two places to look:
> - **Your scheduler's console**, where it appears live within seconds, tagged `[task.stdout]`.  That console also
>   carries ordinary task output, so it is busy; we included `**FINDME**` in the message so `grep FINDME` picks
>   out yours.
> - **`$AIRFLOW_HOME/logs/executor_callbacks/<dag_id>/<run_id>/<callback_id>`**, the durable copy.  Note the
>   filename is the bare callback UUID with no `.log` extension, and the contents are JSON lines.
>
> If the callback ran at all, your scheduler also logs `Callback ... completed successfully`, which is a useful
> check when you cannot find the output itself.

### Finding your scheduler's output

"Watch the scheduler console" means something different depending on how you run Airflow:

| how you run it | where the callback output shows up |
|---|---|
| `airflow standalone` | the same terminal, on lines prefixed `scheduler` |
| `airflow scheduler` in its own terminal | that terminal |
| official Docker Compose | `docker compose logs -f airflow-scheduler` |
| systemd service | `journalctl -u airflow-scheduler -f` |

**If none of those fit, skip the console entirely.**  This works on every install, needs no access to whatever is
running your scheduler, and can be run after the fact:

```bash
grep -r FINDME "$(airflow config get-value logging base_log_folder)/executor_callbacks/"
```

`solutions/cob_reference_step3_registered.py` is the matching known-good example if you want to compare.

## Step 4: Wire it up with a negative interval

Your Reference is working, but the Dag is not finished.  In `dags/cob_deadline_demo_dag.py`, change the interval to a
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

The checker reads the interval out of `dags/cob_deadline_demo_dag.py`, so the `interval` and `deadline` lines now
reflect what you actually wrote.  Before this step it reports `0:30:00` and points out that a positive interval puts
the deadline *after* the reference; afterwards it shows `-1 day, 23:30:00`, which is how a `timedelta` prints minus
thirty minutes.

**In Airflow:** the deadline moves 30 minutes earlier than the close of business your Reference returns.  Step 7 is
where you watch that matter.

`solutions/cob_deadline_demo_dag.py` is the finished Dag.

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
```

Note the decorator order. `@dataclass` is applied first, so registration sees a finished dataclass.

Then read the Variables inside `_evaluate_with()`.  On Airflow 3.3.0 you cannot use `airflow.sdk.Variable.get` for
this, so your skeleton already contains a `get_variable()` helper that stands in for it.  You do not have to write
or change it; just call it:

```python
config = get_variable(self.cob_variable_name, default=DEFAULT_COB, deserialize_json=True, session=session)
```

`deserialize_json=True` parses the JSON for you, and `default=` means an unset Variable gets a documented fallback
rather than a crash.  Both lookups need the `session`, so give your helper methods a keyword-only `session`
parameter and pass it down from `_evaluate_with()`.

> [!IMPORTANT]
> **Why the helper exists.**  `Variable.get()` is the API you would reach for, and the Deadline Alerts docs point at
> it.  The surprise is not that the SDK cannot read a Variable out here; it reads one perfectly well.  It is that
> *every* Variable API in Airflow 3.3.0 opens its **own** database session to do it, and opening a second session
> commits and closes the one your Reference was handed.  Depending on what Airflow was in the middle of, that
> detaches its objects, throws away the writes it makes next, or is rejected outright and reported back to you as
> `VARIABLE_NOT_FOUND` for a Variable that plainly exists.  Add `default=` and the whole thing goes quiet: your
> default comes back and nothing says the lookup failed.  I only found this shortly before the summit, hence the
> workaround.
>
> **The rule worth taking home is not "the SDK can't do this here", it is: reuse the session you were given.**
> `MetastoreBackend` is the only API in 3.3.0 that lets you pass one in.  `airflow.models.Variable.get` looks like
> the obvious alternative and is not; it accepts no session either, so it detaches your objects too.
>
> `default=` is safe in the helper, because `MetastoreBackend` returning nothing genuinely means the Variable is
> unset.  The danger was never `default=`; it was `default=` on top of an error that lied.
>
> Two caveats.  This reads only the metadata database, so `AIRFLOW_VAR_*` environment variables and other secrets
> backends are skipped.  And the helper deliberately mirrors `Variable.get`'s signature, so if the SDK API is fixed
> the upgrade is to delete the helper, rename the call to `Variable.get`, and drop `session=session`.  Nothing else
> changes.

Now let's add the business-day logic: if it is past close of business today, or today is a weekend, roll
forward to the next working day.  Everything you need from `datetime` is already imported.

### Checkpoint

```bash
python checker/check.py
```

Your Reference should now compute a real close of business rather than a date in the past.  The checker will report
the fields it found and flag them as lost, which is Step 6.

**In Airflow:** it runs, and it looks correct.  That is the trap.  The demo Dag passes the same Variable names as
your field defaults, so the missing serializers change nothing you can observe.  Nothing warns you.  The bug only
surfaces the day somebody points the Reference at a different Variable and gets the default anyway.

`solutions/cob_reference_step5_variable.py` is the matching known-good example if you want to compare.

## Step 6: Add the serializers

Your Reference travels with the serialized Dag, and the scheduler rebuilds it from a dict.  The inherited 
`serialize_reference()` carries only the class name, which was fine in Step 3 but is now actively harmful since
your new field gets silently reset to its default on the way back.  So we need to teach Airflow how to handle
it.

```python
def serialize_reference(self) -> dict[str, Any]:
    return {
        "reference_type": self.reference_name,
        "cob_variable_name": self.cob_variable_name,
    }


@classmethod
def deserialize_reference(cls, reference_data: dict[str, Any]) -> CloseOfBusinessDeadline:
    return cls(cob_variable_name=reference_data["cob_variable_name"])
```

### Checkpoint

```bash
python checker/check.py
`````

Back to `All good`, and this time `fields survive` is listed.  That line is the point of the step: the checker sends
probe values through `serialize_reference()` and `deserialize_reference()` and reports any field that comes back
wrong.  Comparing the serialized dict alone would have passed at Step 5 too.

**In Airflow:** it runs, and nothing visibly changes from Step 5.  That is expected, and it is exactly why the
checker exists.

`solutions/cob_reference_step6_final.py` is the finished Reference.

## Step 7: Test both directions

This step needs a running Airflow: it is where you watch a deadline actually fire.  Without one, the checker
already told you what the deadline would be, and you can change the close-of-business time in
`checker/variables.json` to see it move between "already passed" and "in the future".  The live version will be
demonstrated during the session.

**A close of business in the past should fire immediately.**

```bash
airflow variables set cob_config '{"hour": 0, "minute": 1}'
airflow dags trigger cob_deadline_demo
```

The deadline is already overdue, so the callback runs on the next scheduler heartbeat.  Watch your scheduler's
console for `**FINDME**`, or `grep FINDME` the file under `$AIRFLOW_HOME/logs/executor_callbacks/`.

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

- **Holidays from a Variable.**  The exercise skips weekends only.  Add a second field naming a holiday Variable,
  read it in `_evaluate_with()`, and extend `_is_business_day()`.  This is the best test of whether Step 6 stuck:
  a new field is invisible until you remember to carry it through **both** serializers, and the checker will tell
  you if you don't.  `checker/variables.json` already has a `us_holidays` entry; in Airflow, set it with

  ```bash
  airflow variables set us_holidays '["2026-12-25", "2026-01-01", "2026-07-04"]'
  ```
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
- **`Variable.get()` inside `_evaluate_with()` is not safe on any path, and on a scheduled Dag it can take down a
  scheduler.**  The helper in Step 5 is not belt-and-braces; it is load-bearing.  Every Variable API in 3.3.0 opens
  its own database session, which commits and closes the one your Reference was handed, and what you see next
  depends only on what Airflow was in the middle of:

  - On a **manual** run the deadline write can lose the objects it needs and raise `DetachedInstanceError`.  On a
    **scheduled** run, confirmed on 3.3.0, the scheduler's own "do not crash on a misconfigured Dag" handler then
    raises a second error while logging the first, and **the scheduler process exits and crashes again on restart**
    because the same Dag is still due.  If that happens to you: **pause the Dag** (the UI stays up, only the
    scheduler dies), then restart the scheduler.
  - Under the scheduler's `prohibit_commit` guard the read is rejected and reported to you as `VARIABLE_NOT_FOUND`
    for a Variable that plainly exists.  With `default=` set you get your default and **no error at all**.
  - Writes Airflow makes *after* your read can be silently discarded.  A custom **timetable** that reads a Variable
    this way never gets its `next_dagrun` written, so the Dag simply never runs, with nothing logged.  Same defect,
    different feature.

  `airflow.models.Variable.get` is not the way out; it accepts no session either.  Pass the session you were given,
  which is what `get_variable()` does:

  ```python
  from airflow.secrets.metastore import MetastoreBackend

  raw = MetastoreBackend().get_variable("cob_config", session=session)
  ```

  Verified to work both inside and outside the scheduler's guard, leaving the caller's objects intact.
  [#68917](https://github.com/apache/airflow/pull/68917) fixes the equivalent problem for Airflow's own
  Variable-backed interval, but not for a custom Reference doing its own lookup, so this is the pattern to use.
- **An `AIRFLOW_VAR_*` environment variable is a second escape hatch.**  The environment secrets backend is
  consulted before the metadata database and opens no session, so plain `Variable.get("cob_config")` works from
  inside a Reference when the value comes from `AIRFLOW_VAR_COB_CONFIG`.  Two catches: changing it means restarting
  the component, so you lose the "edit it and watch the deadline move" trick this exercise relies on; and because
  the environment wins, an env var left set will silently shadow the Variable you edit in the UI.
- **Put configuration in your Reference, not in the interval.**  Airflow does have a way to read the *interval*
  itself from a Variable, but it is not usable yet: it rejects negative values, so "alert 30 minutes before" is off
  the table, and its behaviour on scheduled runs is still being repaired.  Keeping the configuration inside the
  Reference, which is what this exercise does, gets you Variable-backed timing and negative intervals today.
