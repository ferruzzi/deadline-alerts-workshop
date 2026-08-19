"""
Solution: the finished CloseOfBusinessDeadline.

Reads the close-of-business time from an Airflow Variable, so operations can move the deadline without
editing Dag code, and skips weekends.

Copy this to your plugins folder as ``cob_reference.py`` if you want to compare against your own version.
The plugin that registers it is at the bottom of this same file.
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


@deadline_reference()
@dataclass
class CloseOfBusinessDeadline(BaseDeadlineReference):
    """
    Return the next close of business, skipping weekends.

    The field names an Airflow Variable rather than holding a value directly, which makes the deadline
    reconfigurable at runtime.  Every field needs a default since registration instantiates the class
    with no arguments.
    """

    cob_variable_name: str = "cob_config"

    def _evaluate_with(self, *, session: Session, **kwargs: Any) -> datetime | None:
        hour, minute = self._cob_time(session=session)

        # We are using local time for this demo because it is timezone-aware and does not require a
        # timezone database.  In production, you'd want to also accept a timezone name to apply.
        now = datetime.now().astimezone()
        today = now.date()

        cob_today = self._at(today, hour, minute, now.tzinfo)
        if now < cob_today and self._is_business_day(today):
            return cob_today

        # If it is currently after close of business, or it is not a work day: roll to the next work day.
        day = today + timedelta(days=1)
        while not self._is_business_day(day):
            day += timedelta(days=1)
        return self._at(day, hour, minute, now.tzinfo)

    # -- helpers -- Step 5 -----------------------------------------------------

    @staticmethod
    def _at(day: date, hour: int, minute: int, tzinfo: Any) -> datetime:
        return datetime.combine(day, time(hour, minute), tzinfo=tzinfo)

    @staticmethod
    def _is_business_day(day: date) -> bool:
        # Return True if the day is a weekday or False if it is a weekend.
        # This also serves as a convenient spot to insert the Holidays stretch goal.
        return day.weekday() < 5

    def _cob_time(self, *, session: Session) -> tuple[int, int]:
        # deserialize_json parses the Variable for us, and the default covers the case where nobody has set it yet.
        config = get_variable(self.cob_variable_name, default=DEFAULT_COB, deserialize_json=True, session=session)
        return int(config["hour"]), int(config.get("minute", 0))

    # -- serialization -- Step 6 -----------------------------------------------
    # Without these, the field silently reverts to its default when the scheduler rebuilds the Reference.

    def serialize_reference(self) -> dict[str, Any]:
        return {
            "reference_type": self.reference_name,
            "cob_variable_name": self.cob_variable_name,
        }

    @classmethod
    def deserialize_reference(cls, reference_data: dict[str, Any]) -> CloseOfBusinessDeadline:
        return cls(cob_variable_name=reference_data["cob_variable_name"])


# -- registration -- Step 3 -----------------------------------------------------------
# Required in addition to @deadline_reference(): the scheduler resolves references through the plugin registry, and
# one that is decorated but unregistered raises DeadlineReferenceNotRegistered at Dag run creation.
class CobPlugin(AirflowPlugin):
    name = "cob_plugin"
    deadline_references = [CloseOfBusinessDeadline]
