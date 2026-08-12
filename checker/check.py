#!/usr/bin/env python3
"""
Validate a custom Deadline Reference without installing Airflow.

NOTE:
  This is not production code, it is explicitly written to test the code written to follow this workshop.
  It is brittle and **will** break if you rename files or parameters, or if you get too creative with your code.

Parameters:
  First parameter is the path to the reference plugin file, defaults to ../plugins/cob_reference.py
  Second parameter is the path to the Dag file, defaults to ../dags/cob_deadline_demo.py

Requirements:
  Python 3.10+ and nothing else.

How it works:
  Before importing your file, this script registers stand-in ``airflow.*`` modules in ``sys.modules``.  Your
  file's imports resolve against those stubs, so the exact same file you write here works unchanged on a real
  Airflow install.  Nothing is monkeypatched inside Airflow, because Airflow is not here.

What it checks:
  1. your reference can be constructed with no arguments, which registration requires
  2. ``_evaluate_with()`` returns a timezone-aware datetime
  3. ``serialize_reference()`` and ``deserialize_reference()`` round-trip
  4. your reference is listed in an ``AirflowPlugin.deadline_references``
"""

from __future__ import annotations

import ast
import dataclasses
import json
import sys
import types
from datetime import datetime, timedelta
from pathlib import Path

HERE = Path(__file__).resolve().parent
DEFAULT_INTERVAL = timedelta(minutes=-30)
DEMO_DAG = HERE.parent / "dags" / "cob_deadline_demo.py"


############################################################################
# Stand-ins for the pieces of Airflow a reference touches.  As mentioned
# above, this is not meant for production and these only "mock" the paths
# which are required to participate in the workshop.
############################################################################

_NOTSET = object()
_REGISTERED: list[type] = []


class _Variable:
    """
    Partial mock of ``airflow.sdk.Variable``: only implements ``get``, which is backed by variables.json.
    """

    _store: dict[str, str] | None = None

    @classmethod
    def _load(cls) -> dict[str, str]:
        if cls._store is None:
            path = HERE / "variables.json"
            cls._store = json.loads(path.read_text()) if path.exists() else {}
        return cls._store

    @classmethod
    def get(cls, key: str, default=_NOTSET, deserialize_json: bool = False):
        store = cls._load()
        if key not in store:
            if default is _NOTSET:
                raise KeyError(
                    f"Variable {key!r} is not set.  Add it to {HERE / 'variables.json'}, or pass a default like "
                    f"Variable.get({key!r}, default=...)."
                )
            return default
        raw = store[key]
        return json.loads(raw) if deserialize_json else raw


class _BaseDeadlineReference:
    """
    Partial mock of ``BaseDeadlineReference``: only implements the serializers and the ``reference_name`` property.
    """

    @property
    def reference_name(self) -> str:
        return self.__class__.__name__

    def serialize_reference(self) -> dict:
        return {"reference_type": self.reference_name}

    @classmethod
    def deserialize_reference(cls, reference_data: dict):
        return cls()


def _deadline_reference(deadline_reference_type=None):
    """
    Stand-in for the real decorator, and deliberately stricter than it.

    On Airflow 3.3.0 the bare form silently rebinds the class to a function and fails later somewhere confusing.
    Here it raises immediately, so the checker never lets something pass that a real install would reject.  The
    type argument is accepted and ignored: it changes nothing observable in 3.3.0.
    """
    if isinstance(deadline_reference_type, type):
        raise TypeError(
            "@deadline_reference needs parentheses on Airflow 3.3.0.  Used bare it rebinds your class to a function "
            "and fails later with a confusing error.  Write @deadline_reference() instead."
        )

    def decorator(reference_class: type) -> type:
        _REGISTERED.append(reference_class)
        return reference_class

    return decorator


class _AirflowPlugin:
    """Partial mock of ``AirflowPlugin``: only implements the two attributes the checker reads."""

    name: str = ""
    deadline_references: list = []


def _install_stubs() -> None:
    def module(name: str, **attrs) -> types.ModuleType:
        mod = types.ModuleType(name)
        for key, value in attrs.items():
            setattr(mod, key, value)
        sys.modules[name] = mod
        return mod

    module("airflow")
    module("airflow.plugins_manager", AirflowPlugin=_AirflowPlugin)
    module("airflow.sdk", Variable=_Variable)
    module("airflow.sdk.definitions")
    module(
        "airflow.sdk.definitions.deadline",
        BaseDeadlineReference=_BaseDeadlineReference,
        deadline_reference=_deadline_reference,
    )
    module("airflow.sdk.definitions.variable", Variable=_Variable)
    # Only needed if your file imports Session at runtime instead of guarding it
    # behind TYPE_CHECKING.  Harmless either way.
    module("sqlalchemy")
    module("sqlalchemy.orm", Session=object)


############################################################################
# Code checks
############################################################################


def _interval_from_dag(path: Path) -> tuple[timedelta, str]:
    """
    Read ``interval=timedelta(...)`` out of the first ``DeadlineAlert`` in a Dag file.

    Deliberately fragile and literal-minded: it looks for exactly the shape this exercise uses.  Rename things,
    pull the interval out into a constant, or compute it, and it will fail to find it and fall back to a
    default rather than guess.  It is a convenience helper for people following the guide, and is in no way
    intended to be a general-purpose Dag parser.
    """
    if not path.exists():
        return DEFAULT_INTERVAL, f"no Dag at {path.name}, showing an illustrative interval"

    try:
        tree = ast.parse(path.read_text())
    except SyntaxError:
        return DEFAULT_INTERVAL, f"{path.name} does not parse, showing an illustrative interval"

    for node in ast.walk(tree):
        if not (isinstance(node, ast.Call) and getattr(node.func, "id", None) == "DeadlineAlert"):
            continue
        for keyword in node.keywords:
            if keyword.arg != "interval":
                continue
            value = keyword.value
            if isinstance(value, ast.Call) and getattr(value.func, "id", None) == "timedelta":
                try:
                    kwargs = {kw.arg: ast.literal_eval(kw.value) for kw in value.keywords}
                    return timedelta(**kwargs), f"from {path.name}"
                except (ValueError, TypeError):
                    pass
            return DEFAULT_INTERVAL, f"could not read the interval in {path.name}, showing an illustrative one"

    return DEFAULT_INTERVAL, f"no DeadlineAlert in {path.name}, showing an illustrative interval"


def _load(path: Path) -> types.ModuleType:
    import importlib.util

    spec = importlib.util.spec_from_file_location(path.stem, path)
    if spec is None or spec.loader is None:
        raise SystemExit(f"Could not import {path}")
    mod = types.ModuleType(spec.name)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)  # type: ignore[union-attr]
    return mod


def _find_references(mod: types.ModuleType) -> set[type]:
    found = set(_REGISTERED)
    for value in vars(mod).values():
        if (
            isinstance(value, type)
            and issubclass(value, _BaseDeadlineReference)
            and value is not _BaseDeadlineReference
        ):
            found.add(value)
    return found


def _registered_in_plugin(mod: types.ModuleType, cls: type) -> bool:
    for value in vars(mod).values():
        if isinstance(value, type) and issubclass(value, _AirflowPlugin) and value is not _AirflowPlugin:
            if cls in getattr(value, "deadline_references", []):
                return True
    return False


def _check(mod: types.ModuleType, cls: type, interval: timedelta, interval_source: str) -> list[str]:
    problems: list[str] = []
    print(f"\n=== {cls.__name__} ===")

    if cls not in _REGISTERED:
        problems.append(f"{cls.__name__} is missing the @deadline_reference() decorator.")

    try:
        instance = cls()
    except TypeError as err:
        problems.append(
            f"{cls.__name__}() failed: {err}.  Registration constructs your class with no "
            "arguments, so every field needs a default."
        )
        return problems

    # 1. evaluate
    try:
        result = instance._evaluate_with(session=None)
    except NotImplementedError:
        problems.append(
            f"{cls.__name__}._evaluate_with() is not implemented yet.  That is Step 2: return a "
            "timezone-aware datetime that has already passed."
        )
        return problems
    except Exception as err:
        problems.append(
            f"{cls.__name__}._evaluate_with() raised {type(err).__name__}: {err}"
        )
        return problems

    print(f"  reference          {result}")
    if result is None:
        problems.append("_evaluate_with() returned None, so no deadline would be created.")
        return problems
    if not isinstance(result, datetime):
        problems.append(f"_evaluate_with() returned {type(result).__name__}, expected datetime.")
        return problems
    if result.tzinfo is None:
        problems.append(
            "_evaluate_with() returned a naive datetime.  Deadlines are stored as UTC, so an "
            "aware datetime is required; datetime.now().astimezone() is the easy fix."
        )
    else:
        # 2. the deadline Airflow would store
        deadline = result + interval
        now = datetime.now().astimezone()
        verdict = "already passed, fires immediately" if deadline < now else "in the future"
        print(f"  interval           {interval}  ({interval_source})")
        print(f"  deadline           {deadline}  ({verdict})")
        if interval > timedelta(0):
            print("                     a positive interval puts the deadline AFTER the reference; Step 4")
            print("                     makes it negative so the alert arrives early")

    # 3. serialization round-trip
    payload = instance.serialize_reference()
    print(f"  serialized         {payload}")
    rebuilt = cls.deserialize_reference(payload)
    if rebuilt.serialize_reference() != payload:
        problems.append(
            "serialize/deserialize did not round-trip.\n"
            f"      before: {payload}\n"
            f"      after:  {rebuilt.serialize_reference()}\n"
            "    Every field you added needs to appear in serialize_reference() and be read "
            "back in deserialize_reference()."
        )
    else:
        print("  round-trip         ok")

    # 3b. do the field VALUES survive?  A class with fields and the inherited
    # serializers round-trips cleanly while silently resetting every field to its
    # default, which is the whole reason Step 6 exists.
    if dataclasses.is_dataclass(cls) and dataclasses.fields(cls):
        probes = {field.name: f"probe-{field.name}" for field in dataclasses.fields(cls)}
        probe = dataclasses.replace(instance, **probes)
        probe_payload = probe.serialize_reference()
        probe_rebuilt = cls.deserialize_reference(probe_payload)
        lost = [name for name, value in probes.items() if getattr(probe_rebuilt, name, None) != value]
        if lost:
            problems.append(
                f"{cls.__name__} loses field(s) {', '.join(sorted(lost))} when serialized.\n"
                "    The inherited serializers only carry the class name, so your fields silently\n"
                "    revert to their defaults on the way back.  That is Step 6: write\n"
                "    serialize_reference() and deserialize_reference() to carry them."
            )
        else:
            print(f"  fields survive     ok ({', '.join(sorted(probes))})")

    # 4. plugin registration
    if _registered_in_plugin(mod, cls):
        print("  plugin             registered")
    else:
        problems.append(
            f"{cls.__name__} is not listed in any AirflowPlugin.deadline_references.  The "
            "decorator alone is not enough: on a real install this raises "
            "DeadlineReferenceNotRegistered when the Dag run is created."
        )

    return problems


def main() -> int:
    _install_stubs()

    target = Path(sys.argv[1]) if len(sys.argv) > 1 else HERE.parent / "plugins" / "cob_reference.py"
    target = target.resolve()
    if not target.exists():
        raise SystemExit(f"No such file: {target}")

    dag_path = (Path(sys.argv[2]) if len(sys.argv) > 2 else DEMO_DAG).resolve()
    interval, interval_source = _interval_from_dag(dag_path)

    print(f"Checking {target}")
    try:
        mod = _load(target)
    except Exception as err:
        print(f"\n{target.name} could not be imported.\n\n  {type(err).__name__}: {err}\n")
        print("Fix that first; nothing else can be checked until the file imports cleanly.")
        return 1

    references = _find_references(mod)
    if not references:
        raise SystemExit("No BaseDeadlineReference subclass found in that file.")

    problems: list[str] = []
    for cls in references:
        problems.extend(_check(mod, cls, interval, interval_source))

    print()
    if problems:
        print(f"{len(problems)} thing(s) to fix:\n")
        for problem in problems:
            print(f"  - {problem}")
        return 1

    print(f"All good: {len(references)} reference(s) checked.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
