# Advanced Deadline Alerts: Airflow Summit 2026 Workshop

Materials for the **Advanced Deadline Alerts** hands-on workshop at Airflow Summit 2026 (1 September 2026).

In this workshop you will build a custom Deadline Reference from scratch: a `CloseOfBusinessDeadline` that 
knows your business calendar, skips weekends and holidays, and warns you *before* the deadline rather than after.

## Before You Arrive

**Please set this up before the session.** Conference wifi will not be able to support 25 people downloading 
Airflow at once, and we only have two hours.

### Requirements

- **Apache Airflow 3.3.0 or newer**. Any installation method is fine: `pip` in a virtualenv, Docker, 
  `airflow standalone`, an existing dev environment, or Breeze if that is already your workflow.
- Ability to **add a file to your plugins directory** and restart Airflow.
- Ability to **set an Airflow Variable** (UI or CLI).
- Ability to **read task logs**, which is where the deadline callback output appears.
- **Python 3.10 or newer**.  If Airflow runs you already have this. Listed separately because the minimalist 
  path below needs only Python.

Airflow 3.3.0 is the floor because the workshop uses features that landed across the 3.x line: multiple 
deadline alerts per Dag (3.2.0) and `VariableInterval` (3.3.0).

### Verify Your Setup

Run this against the same Python environment as your Airflow install:

```bash
python -c "from airflow.sdk.definitions.deadline import VariableInterval; print('ready')"
```

If it prints `ready`, you are set. If it raises `ImportError` or `ModuleNotFoundError`, your Airflow is 
either older than 3.3.0 or not installed in that environment.

Also confirm the version itself:

```bash
airflow version
```

### Get the Materials

Clone this repo ahead of time as well. It is only a few kilobytes, so it is not the bandwidth problem the 
Airflow install is, but having it already on disk is one less thing to do in the room:

```bash
git clone https://github.com/ferruzzi/deadline-alerts-workshop.git
```

Exercise and solution files are still being finalized, so **run `git pull` the morning of the session** 
to pick up the latest.

## Didn't Get Set Up? You Can Still Participate

Two fallbacks, no preparation required:

1. **Pair up.** One working laptop per pair is plenty, and talking through the logic with someone else is 
   arguably the better way to learn it.  We will try to pair people off at the start of the session.
2. **Minimalist path.** The important part of this exercise, the business-day logic and the serialization 
   contract, does not need a running Airflow at all. See [`minimalist/README.md`](minimalist/README.md) for a standalone 
   runner that needs only Python 3.10+ and no Airflow install.  You lose the option to see your work run
   in the Airflow UI, but it can still be validated.

## Useful Links

- [Deadline Alerts documentation](https://airflow.apache.org/docs/apache-airflow/stable/howto/deadline-alerts.html)
- [Airflow Summit 2026](https://airflowsummit.org/)

## Questions

Find me at the Summit, or `@ferruzzi` on the
[Apache Airflow Slack](https://apache-airflow-slack.herokuapp.com/).

## License

Apache License 2.0. See [LICENSE](LICENSE).
