from sqlalchemy import func, select
from sqlalchemy.orm import Session

from .models import Job, JobState


def snapshot(session: Session) -> dict:
    counts = {state.value: 0 for state in JobState}
    for state, count in session.execute(select(Job.state, func.count()).group_by(Job.state)):
        counts[state.value] = count
    return {"jobs_by_state": counts, "submitted_jobs": sum(counts.values()),
            "completed_jobs": counts["succeeded"], "failed_jobs": counts["failed"],
            "dead_letter_count": counts["dead_letter"], "active_leases": counts["leased"],
            "retry_count": session.scalar(select(func.sum(Job.attempt_count))) or 0}

