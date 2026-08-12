"""
Solution: the finished CloseOfBusinessDeadline.

Reads the close-of-business time from an Airflow Variable, so operations can move the deadline without
editing Dag code, and skips weekends and holidays.

Copy this to your plugins folder as ``cob_reference.py`` if you want to compare against your own version.
The plugin that registers it is at the bottom of this same file.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from typing import TYPE_CHECKING, Any

from airflow.plugins_manager import AirflowPlugin
from airflow.sdk import Variable
from airflow.sdk.definitions.deadline import BaseDeadlineReference, deadline_reference

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

# Used when the Variable is absent, so an unset Variable is a documented default rather than a crash mid-exercise.
DEFAULT_COB = {"hour": 17, "minute": 0}


@deadline_reference()
@dataclass
class CloseOfBusinessDeadline(BaseDeadlineReference):
    """
    Return the next close of business, skipping weekends and holidays.

    Both fields name Airflow Variables rather than holding values directly, which is what makes the deadline
    reconfigurable at runtime.  Every field needs a default; registration instantiates the class with no arguments.
    """

    cob_variable_name: str = "cob_config"
    holidays_variable_name: str | None = "us_holidays"

    def _evaluate_with(self, *, session: Session, **kwargs: Any) -> datetime | None:
        hour, minute = self._cob_time()
        holidays = self._holidays()

        # We are using local time for this demo because it is timezone-aware and does not require a timezone database.
        # In production, you'd likely want to also accept a timezone name and apply that.
        now = datetime.now().astimezone()
        today = now.date()

        cob_today = self._at(today, hour, minute, now.tzinfo)
        if now < cob_today and self._is_business_day(today, holidays):
            return cob_today

        # If it is currently after close of business, or it is not a work day: roll to the next work day.
        day = today + timedelta(days=1)
        while not self._is_business_day(day, holidays):
            day += timedelta(days=1)
        return self._at(day, hour, minute, now.tzinfo)

    # -- helpers ---------------------------------------------------------------

    @staticmethod
    def _at(day: date, hour: int, minute: int, tzinfo: Any) -> datetime:
        return datetime.combine(day, time(hour, minute), tzinfo=tzinfo)

    @staticmethod
    def _is_business_day(day: date, holidays: set[date]) -> bool:
        is_weekday = day.weekday() < 5
        return is_weekday and day not in holidays

    def _cob_time(self) -> tuple[int, int]:
        # deserialize_json parses the Variable for us, and the default covers the case where nobody has set it yet.
        # Note: on *scheduled* Dag runs this call has a known limitation.  See the gotchas in EXERCISE.md.
        config = Variable.get(self.cob_variable_name, default=DEFAULT_COB, deserialize_json=True)
        return int(config["hour"]), int(config.get("minute", 0))

    def _holidays(self) -> set[date]:
        if not self.holidays_variable_name:
            return set()
        raw = Variable.get(self.holidays_variable_name, default=[], deserialize_json=True)
        return {date.fromisoformat(day) for day in raw}

    # -- serialization ---------------------------------------------------------
    # Since we have added fields to our custom Reference, we now need to tell Airflow how to serialize and
    # deserialize them since the Reference travels with the serialized Dag.  The default implementations on
    # BaseDeadlineReference only carry the class name, which is enough for a reference with no fields but
    # would silently drop both of ours.

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


# -- registration ---------------------------------------------------------------------
# The decorator above makes the class usable in a Dag file as DeadlineReference.CloseOfBusinessDeadline, but the
# scheduler resolves references through the plugin registry when it deserializes the Dag.  A reference that is
# decorated but not listed here raises DeadlineReferenceNotRegistered at Dag run creation, which fails the whole
# run rather than just the Deadline.
class CobPlugin(AirflowPlugin):
    name = "cob_plugin"
    deadline_references = [CloseOfBusinessDeadline]
