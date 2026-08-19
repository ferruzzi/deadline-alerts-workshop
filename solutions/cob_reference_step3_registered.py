"""
Solution: Step 3, the smallest thing that fires.

At this point the reference takes no arguments and returns a close of business that has already passed, so the
Deadline is overdue the moment the Dag run is created and the callback fires on the next scheduler heartbeat.
That fast feedback loop is the whole point of doing this step first.

Because there are no fields, the inherited ``serialize_reference`` and ``deserialize_reference`` are sufficient:
they carry the class name, which is all the scheduler needs to rebuild an argument-less reference.  Serializers
become necessary in the next step, when the class gains fields.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from typing import TYPE_CHECKING, Any

from airflow.plugins_manager import AirflowPlugin
from airflow.sdk.definitions.deadline import BaseDeadlineReference, deadline_reference
from airflow.secrets.metastore import MetastoreBackend

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

# ---------------------------------------------------------------------------------------------------------------
# PROVIDED FOR YOU.  You never need to edit this helper, and you will not need it until Step 5.
#
# It stands in for ``Variable.get`` because on Airflow 3.3.0 that call is not usable in here: every Variable API
# opens its own database session, which commits and closes the one your Reference was handed.  The rule to take
# home is "reuse the session you were given"; EXERCISE.md Step 5 has the full story.
# ---------------------------------------------------------------------------------------------------------------
_NOTSET = object()


def get_variable(key: str, default: Any = _NOTSET, deserialize_json: bool = False, *, session: Session) -> Any:
    """Read an Airflow Variable using the provided session."""
    raw = MetastoreBackend().get_variable(key, session=session)
    if raw is None:
        if default is _NOTSET:  # no default was passed, so an unset Variable is an error, not a silent None
            raise KeyError(f"Variable {key!r} is not set, and no default was given.")
        return default
    return json.loads(raw) if deserialize_json else raw


# ---------------------------------------------------------------------------------------------------------------
# EDIT BELOW THIS LINE
# ---------------------------------------------------------------------------------------------------------------

# Used when the Variable is absent, so an unset Variable is a documented default rather than a crash mid-exercise.
DEFAULT_COB = {"hour": 17, "minute": 0}


# Apollo 11 touchdown.  Absurd on purpose: nobody will mistake this for business logic,
# which is exactly what you want from temporary scaffolding.
MOON_LANDING = datetime(1969, 7, 20, 20, 17, tzinfo=timezone.utc)


@deadline_reference()
class CloseOfBusinessDeadline(BaseDeadlineReference):
    """A close of business that is permanently, obviously in the past."""

    def _evaluate_with(self, *, session: Session, **kwargs: Any) -> datetime | None:
        # The one hard requirement is that this is timezone-aware.  Deadlines are stored as UTC
        # timestamps, so a naive datetime would be wrong.  Here that comes from tzinfo=timezone.utc;
        # once you need "now", datetime.now().astimezone() gets you an aware value just as easily.
        return MOON_LANDING


# -- registration -- Step 3 -----------------------------------------------------------
# Required in addition to @deadline_reference(): the scheduler resolves references through the plugin registry, and
# one that is decorated but unregistered raises DeadlineReferenceNotRegistered at Dag run creation.
class CobPlugin(AirflowPlugin):
    name = "cob_plugin"
    deadline_references = [CloseOfBusinessDeadline]
