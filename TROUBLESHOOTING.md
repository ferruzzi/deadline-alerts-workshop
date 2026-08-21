# Troubleshooting

Deadline Alerts are new, and several things in Airflow 3.3.0 fail *silently* rather than with an error.  If
something in this workshop appears to do nothing, look here before you go hunting through your own code.

**Everything below was observed on Airflow 3.3.0**, which is what the workshop targets.  I try to say whether
each issue is fixed or being fixed upstream, but things change fast so this may be out of date.

Ordered roughly by how likely you are to hit it.

- [Nothing happened when I triggered the Dag](#nothing-happened-when-i-triggered-the-dag)
  - [The Dag is paused](#the-dag-is-paused)
  - [You edited a plugin and did not restart](#you-edited-a-plugin-and-did-not-restart)
- [Where does the callback output actually go?](#where-does-the-callback-output-actually-go)
- [`DeadlineReferenceNotRegistered`, but I did register it](#deadlinereferencenotregistered-but-i-did-register-it)
  - [You added the plugin while Airflow was running](#you-added-the-plugin-while-airflow-was-running)
  - [Two plugins share the same `name`](#two-plugins-share-the-same-name)
- [My Reference reads a Variable and the value is ignored](#my-reference-reads-a-variable-and-the-value-is-ignored)
- [The scheduler died, or is crash-looping](#the-scheduler-died-or-is-crash-looping)
- [My deadline was in the future, the Dag finished, and nothing ever fired](#my-deadline-was-in-the-future-the-dag-finished-and-nothing-ever-fired)
- [I edited my Dag file and now the deadline never fires](#i-edited-my-dag-file-and-now-the-deadline-never-fires)
- [How you trigger changes which References work](#how-you-trigger-changes-which-references-work)
- [Do not clear a run that has a pending `DAGRUN_QUEUED_AT` deadline](#do-not-clear-a-run-that-has-a-pending-dagrun_queued_at-deadline)
- [No callback at all, and the scheduler is throwing errors](#no-callback-at-all-and-the-scheduler-is-throwing-errors)
- [Smaller oddities](#smaller-oddities)
- [Still stuck?](#still-stuck)

---

## Nothing happened when I triggered the Dag

**Two causes, and between them they account for most of the confusion in this workshop.**

### The Dag is paused

New Dags arrive paused (`core.dags_are_paused_at_creation` defaults to `True`), so this is the default state of
every Dag you write today.  A triggered run on a paused Dag sits in `queued` forever with no start time, and no
task ever starts.

```bash
airflow dags unpause cob_deadline_demo
```

The toggle on the Dag page does the same thing.  Unpausing releases the stuck run immediately.

Confusingly, the *deadline* machinery still works while the Dag does nothing: the deadline row is created when
the run is created, and it will go on to be marked missed while the run sits queued.  So you can see deadline
activity from a Dag that has not executed a single task.

### You edited a plugin and did not restart

Plugin registration happens at import, and the registry is cached **per process**, so a change to a file in
`plugins/` does nothing until the processes that use it are restarted.

If you run `airflow standalone`, that is the whole thing: restart it.  If you run components separately (Breeze, 
Docker, etc), you can either restart the whole thing or just the affected component, depending on what you are doing:

| What You Changed                                | What To Restart           | Why |
|-------------------------------------------------|---------------------------|---|
| A Reference, triggering from the UI or REST API | dag processor, api-server | the dag processor imports your plugin to parse the Dag; the api-server creates the run, so that is where your Reference is instantiated |
| A Reference, and the run comes from a schedule  | dag processor, scheduler  | same, except the scheduler creates the run, then detects the deadline and dispatches the callback |
| A Reference, triggering from the CLI            | dag processor             | `dags trigger` and `dags test` are fresh processes, so they always have your latest code |
| A `SyncCallback`                                | nothing                   | it runs in a fresh process every time it fires |
| An `AsyncCallback`                              | triggerer                 | it runs inside the long-lived triggerer |
| A Dag file in `dags/`                           | nothing                   | the dag processor re-parses it on its own |

A note on the dag processor:  by default it will not re-parse the same file more often than every 30 seconds, and 
it only goes looking for *new* files every 5 minutes.  A file you have just saved is exempt from the 30 second 
hold-off, so an edit to a file Airflow already knows about usually lands on the next loop.  If you don't want to 
wait, restarting it takes seconds and forces a re-parse.

---

## Where does the callback output actually go?

Two places:

1. **Your scheduler's console.**  Look for `DEADLINE MISSED`.  That console carries task output too, so it is noisy.
   Callback lines are the ones with no `dag_id` / `task_id` / `run_id` fields attached.
2. **A file**, at `<base_log_folder>/executor_callbacks/<dag_id>/<run_id>/<callback_id>`.

**If you cannot find your console, use the file instead:**

```bash
grep -r FINDME "$(airflow config get-value logging base_log_folder)/executor_callbacks/"
```

Two things about that file catch people out: it is named as a bare UUID with **no `.log` extension**, so
`find -name '*.log'` will not match it, and its contents are JSON lines rather than plain text.  Your `print()`
output is in there as a `task.stdout` event.

There is **no UI page for callback output** in 3.3.0.  If you only want to know whether the callback ran at all,
the scheduler logs `Callback executed successfully` when it does.

All of the above is for a `SyncCallback`, which is what this exercise uses.  An `AsyncCallback` runs in the
triggerer instead, and its output goes to the triggerer's own log with no per-callback file of its own.

---

## `DeadlineReferenceNotRegistered`, but I *did* register it

The error name can be slightly misleading, and there are two different causes.

### You added the plugin while Airflow was running

#### The Fix:

**Restart the api-server** (and the scheduler and dag processor; see the restart list above).  If this message
appears *after* you added the plugin, restart before believing it.

#### The Reason:

The plugin registry is cached per process, so a long-running api-server or scheduler keeps failing no matter what
is on disk.  A trigger from the UI or REST API returns **HTTP 400** and **creates no Dag run at all**, while the
same Dag triggered from the CLI succeeds, because the CLI is a fresh process.

### Two plugins share the same `name`

#### How to Diagnose:

```bash
airflow plugins    # is your plugin listed, under the name you expect?
```

or, in a Python shell with the same `AIRFLOW__CORE__PLUGINS_FOLDER`:

```python
from airflow import plugins_manager
plugins_manager.ensure_plugins_loaded()
print(plugins_manager.get_deadline_references_plugins())   # the authoritative registry
```

A class that shows up on `DeadlineReference` but not in that dict is exactly this bug. 

#### The Fix:

Give every plugin a distinct `name`.

#### The Reason:

The plugins manager keeps one plugin per `name` attribute.  If two files in your plugins folder both declare, say,
`name = "my_plugin"`, only one survives and the other's `deadline_references` are never registered.  Nothing is
logged and there is no import error.

This is easy to hit by copying two examples from the slides into one plugins folder.  Different class names are
not enough; it is the `name` **string** that collides.

What makes it genuinely confusing is that the `@deadline_reference()` decorator still runs at import, so the lost
class *is* available as `DeadlineReference.MyRef`, and your Dag file parses and serializes without complaint.  It
fails later, when the Dag run is created.

---

## My Reference reads a Variable and the value is ignored

**This is not your code.**

#### How to Diagnose:

Note where the deadline currently lands, then set the Variable to a time nowhere near your fallback and
re-trigger:

```bash
airflow variables set cob_config '{"hour": 11, "minute": 0}'
```

If the deadline does not move, the read is not happening.

Pick the new value carefully.  Our example has you set `{"hour": 17, "minute": 0}`, and `DEFAULT_COB` in 
the skeleton is also 17:00.  As long as those two agree, a failed read lands the deadline in exactly the same 
place as a successful one and proves nothing either way.

#### The Workaround:

**Use the `get_variable()` helper provided in `plugins/cob_reference.py`.**  It wraps the API that works 
(`MetastoreBackend().get_variable(key, session=session)`) using the session `_evaluate_with()` already receives.

#### The Reason:

In 3.3.0, every Variable API asks for a database session of its own, and because sessions are thread-scoped it is 
handed the very session `_evaluate_with()` is using, then commits and closes it as if it owned it.  What you see 
depends on what Airflow happened to be doing:

- Under the scheduler, the read is rejected and reported back as `VARIABLE_NOT_FOUND` for a Variable that plainly
  exists.  If you passed `default=`, you silently get your default instead.
- Elsewhere, the read succeeds but detaches the objects your caller was using, which can take the scheduler down
  (see the next entry).

Note that using `airflow.models.Variable.get` is the tempting fix, but has the same problem.

*Upstream: I will submit a bug fix after Summit.*

---

## The scheduler died, or is crash-looping

No new scheduled runs for *any* Dag, runs stranded in `running`, and the scheduler process exiting and exiting
again on restart.  The UI stays up, because the api-server is unaffected.

#### The Workaround:

Pause the offending Dag from the UI first (the api-server still works, so the toggle still works), *then* restart
the scheduler.  Restarting first just crashes it again on the same due Dag.

#### The Reason:

A scheduled Dag whose Reference reads a Variable inside `_evaluate_with()`.  The Variable read detaches the 
objects the scheduler was working with, and the scheduler's own "do not crash on a misconfigured Dag" handler then 
fails the same way while trying to log it.

Manual and UI triggers of the same Dag are safe, because only the scheduler's run-creation path is affected.  So
"it worked when I clicked Trigger" is expected, and is not evidence against this.

The workshop exercise uses `schedule=None` and manual triggers, so you will not hit this during the guided steps.

*Upstream: same root cause as the Variable entry above.*

---

## My deadline was in the future, the Dag finished, and nothing ever fired

**Working as designed.**  When a Dag run succeeds, Airflow prunes every deadline on that run that has not already
been breached.  A Dag that finishes in two seconds will delete its own close-of-business deadline long before that 
deadline could ever be missed.

#### The Fix:

Give the Dag something slow to do, which is exactly why the demo Dag runs `sleep 300`.  Either the run has to
still be going when the deadline passes, or the deadline has to already be in the past when the run starts.

#### How to Diagnose:

The run is `success`, no deadline row is left for it, and there is no callback output anywhere.  A deadline that
*was* breached survives the prune, so "successful run, no deadline row, no callback" is this rather than the
stale-definition bug below.

---

## I edited my Dag file and now the deadline never fires

**Known 3.3.0 bug.**  Renaming a task or adding a comment is enough to trigger it.

#### The Workaround:

Change the deadline itself.  Nudge the `interval` by a minute, or change the reference arguments, or the callback.
That forces the deadline to be regenerated and it starts working again.

#### How to Diagnose:

The run appears normally and the task runs, but the UI shows no deadline for that run.  No error, nothing in the
logs.

#### The Reason:

When a new Dag version's deadline definition is unchanged, the old deadline row stays attached to the previous
version, while run creation looks it up against the current one.

*Upstream: fixed by [#68734](https://github.com/apache/airflow/pull/68734), merged to `main` and milestoned for
3.3.2, which is not released yet.  Every 3.3.x you can install today has the bug.*

---

## How you trigger changes which References work

Each way of triggering a Dag leaves a different column NULL, and each NULL silently disables a different 
built-in Reference:

| Trigger Path                           | `queued_at` | `logical_date` | Silently Breaks       |
|----------------------------------------|---|---|-----------------------|
| `airflow dags trigger <dag_id>` (bare) | set | **NULL** | `DAGRUN_LOGICAL_DATE` |
| `airflow dags test <dag_id>`           | **NULL** | set | `DAGRUN_QUEUED_AT`    |
| UI trigger button                      | set | set | nothing               |

#### The Fix:

The UI button is the only recipe that is safe for every Reference.  If you are using the CLI, pass a logical 
date explicitly:

```bash
airflow dags trigger <dag_id> -l "$(date -Iseconds)"
```

#### How to Diagnose:

No deadline at all, and no error.  The only log line is a `Could not find DagRun` warning naming a Dag run that
plainly exists, which sends you looking for the wrong problem.

#### The Reason:

A `DAGRUN_LOGICAL_DATE` deadline evaluates to `None` on a NULL logical date, so nothing is written.

`logical_date` is also always NULL for asset-triggered runs, and for partitioned Dags triggered from the UI
(the form hides the field).

`airflow dags test` *does* fire deadline callbacks, but only when a scheduler is running alongside it; detection
is a scheduler-loop job.

None of this affects the workshop's own Reference, which computes its time from the business calendar and reads
neither column.

*Upstream: reported at [#71750](https://github.com/apache/airflow/issues/71750); the misleading warning is being
addressed in [#71767](https://github.com/apache/airflow/pull/71767).  Passing `-l` will remain the right thing to
do regardless.*

---

## Do not clear a run that has a pending `DAGRUN_QUEUED_AT` deadline

`TypeError: unsupported type for timedelta seconds component: dict`, out of the Clear button, the API clear
endpoints, or the CLI.

#### The Workaround:

Re-trigger instead.  Nothing is damaged: the transaction rolls back, the task is left as it was, and the deadline
row is untouched.  Clearing is simply impossible while that deadline is pending, so there is nothing to repair.

#### The Reason:

All three conditions have to line up: a deadline row exists, at least one is un-breached, and its reference is the
built-in queued-at one.  Once the deadline has been missed, clearing works again.  Rows written before 3.3.0 are
fine, which is why it can look like it worked last time.

*Upstream: fix open at [#70370](https://github.com/apache/airflow/pull/70370), milestoned for 3.3.2 and labeled
for backport.*

---

## No callback at all, and the scheduler is throwing errors

**Your executor may not support callbacks.**  The symptom looks nothing like the cause: the deadline is created
and correctly marked missed, and the failure happens afterwards, in the dispatch step.

#### How to Diagnose:

```bash
airflow config get-value core executor
```

- **Works:** LocalExecutor, CeleryExecutor, and the AWS executors.  Celery covers anyone using the official
  Docker Compose file, and `airflow standalone` uses LocalExecutor.
- **Does not:** KubernetesExecutor and Edge3 on 3.3.0.

#### The Fix:

Switch to LocalExecutor, or pair up with someone.  No amount of debugging your Reference will help.

#### The Reason:

Callback support is opt-in, so the scheduler raises `NotImplementedError` when it tries to dispatch a
`SyncCallback` on an executor that does not implement it.

---

## Smaller oddities

Not on the slides, but you may run into some of these if you go exploring.

**`VariableInterval` rejects negative and zero values.**  `ValueError: VariableInterval '<key>' must be > 0`,
raised when the Dag run is created rather than at parse time, so the Dag file looks fine and the failure shows up
on trigger.  Negative intervals, the "warn me *before*" trick this workshop is built on, only work with a literal
`timedelta` for now.

*Upstream: `VariableInterval` is still in active development and this will be fixed.*

**`DAGRUN_QUEUED` does nothing on a custom Reference.**  The timing category you pass to `@deadline_reference()` is
never stored anywhere, and the code that re-calculates queued-at deadlines when a run is cleared picks them out by
class name.  This means if you clear a run with a built-in `DAGRUN_QUEUED_AT` deadline, it moves to the new 
`queued_at`, but a custom reference does not.  For now, use the default `DAGRUN_CREATED` or leave it blank since 
that is what a bare `@deadline_reference()` uses.

*Upstream: reported at [#71747](https://github.com/apache/airflow/issues/71747); the right shape of the fix is
still under discussion.*

---

## Still stuck?

Grab me in the room, or `@ferruzzi` on the [Apache Airflow Slack](https://apache-airflow-slack.herokuapp.com/).
