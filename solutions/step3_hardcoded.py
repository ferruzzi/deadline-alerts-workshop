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

from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from airflow.plugins_manager import AirflowPlugin
from airflow.sdk.definitions.deadline import BaseDeadlineReference, deadline_reference

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

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


# -- registration ---------------------------------------------------------------------
# The decorator above makes the class usable in a Dag file as DeadlineReference.CloseOfBusinessDeadline, but the
# scheduler resolves references through the plugin registry when it deserializes the Dag.  A reference that is
# decorated but not listed here raises DeadlineReferenceNotRegistered at Dag run creation, which fails the whole
# run rather than just the Deadline.
class CobPlugin(AirflowPlugin):
    name = "cob_plugin"
    deadline_references = [CloseOfBusinessDeadline]
