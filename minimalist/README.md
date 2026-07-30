# Minimalist Path (No Airflow Required)

If you could not get Airflow running, you can still do the substantive part of this exercise.  Writing a 
custom Deadline Reference is mostly business-day arithmetic, timezone handling, and a small serialization 
contract.  None of that needs a scheduler.

## Requirements

Python 3.10 or newer.  Nothing else.

## Usage (Work in progress)

```bash
python runner.py
```

The runner stubs out the pieces which Airflow would normally supply, calls your `_evaluate_with()`, and 
prints the resulting deadline.  It also passes your reference through `serialize_reference()` and 
`deserialize_reference()` to confirm the contract holds.

## What You Miss

You will not see a deadline actually fire, since that needs a running scheduler and executor, but 
it will be demonstrated live during the session.

## Afterwards

Everything you write here transfers unchanged to a real Airflow install.  When you have one available, drop 
your file into the plugins directory and follow the main [README](../README.md).
