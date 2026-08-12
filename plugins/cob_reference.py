"""
Your working file.  The walkthrough and all of the explanation live in EXERCISE.md.

Copy this directory into your Airflow plugins folder, and restart Airflow after every change:
plugin registration happens at import.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

# TODO (step 3): import AirflowPlugin from airflow.plugins_manager
from airflow.sdk.definitions.deadline import BaseDeadlineReference, deadline_reference

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


# TODO (step 3): decorate with @deadline_reference()
class CloseOfBusinessDeadline(BaseDeadlineReference):
    """Return the time the report is due: the next close of business."""

    def _evaluate_with(self, *, session: Session, **kwargs: Any) -> datetime | None:
        # TODO (step 2): return a timezone-aware datetime that has already passed
        raise NotImplementedError("Step 2: return an aware datetime in the past")


# TODO (step 3): register the class so the scheduler can resolve it
#
#   class CobPlugin(AirflowPlugin):
#       name = "cob_plugin"
#       deadline_references = [CloseOfBusinessDeadline]


# TODO (step 5): read the close-of-business time from a Variable, and skip weekends and holidays
# TODO (step 6): add serialize_reference() and deserialize_reference()
