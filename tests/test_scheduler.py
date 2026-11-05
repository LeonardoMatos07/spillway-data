from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from energy_pipeline import cron, scheduler
from energy_pipeline.jobs import JOBS, Job


def at(year: int, month: int, day: int, hour: int = 0, minute: int = 0) -> datetime:
    return datetime(year, month, day, hour, minute, tzinfo=UTC)


def test_every_job_has_a_valid_schedule() -> None:
    for job in JOBS.values():
        cron.parse(job.schedule)


def test_monthly_schedule_fires_once_a_month() -> None:
    assert cron.matches("0 6 3 * *", at(2026, 11, 3, 6, 0))
    assert not cron.matches("0 6 3 * *", at(2026, 11, 4, 6, 0))
    assert not cron.matches("0 6 3 * *", at(2026, 11, 3, 6, 1))


def test_aliases_and_steps() -> None:
    assert cron.matches("@hourly", at(2026, 11, 3, 17, 0))
    assert not cron.matches("@hourly", at(2026, 11, 3, 17, 30))
    assert cron.matches("@monthly", at(2026, 12, 1, 0, 0))
    assert cron.matches("*/15 * * * *", at(2026, 11, 3, 1, 45))
    assert not cron.matches("*/15 * * * *", at(2026, 11, 3, 1, 50))
    assert cron.matches("0 9 * * 1-5", at(2026, 11, 4, 9, 0))  # a Wednesday
    assert not cron.matches("0 9 * * 1-5", at(2026, 11, 8, 9, 0))  # a Sunday


def test_weekday_zero_is_sunday_and_seven_too() -> None:
    sunday = at(2026, 11, 1, 10, 0)
    monday = at(2026, 11, 2, 10, 0)
    assert cron.matches("0 10 * * 0", sunday)
    assert cron.matches("0 10 * * 7", sunday)
    assert cron.matches("0 10 * * 1", monday)
    assert not cron.matches("0 10 * * 1", sunday)


def test_day_of_month_or_weekday_when_both_are_set() -> None:
    assert cron.matches("0 0 13 * 5", at(2026, 11, 13))  # the 13th (also a Friday)
    assert cron.matches("0 0 13 * 5", at(2026, 11, 6))  # a Friday
    assert not cron.matches("0 0 13 * 5", at(2026, 11, 7))


@pytest.mark.parametrize("expression", ["* * *", "61 * * * *", "*/0 * * * *", "5-2 * * * *"])
def test_invalid_expressions_are_rejected(expression: str) -> None:
    with pytest.raises(ValueError):
        cron.parse(expression)


def test_due_jobs_at_the_third_of_the_month() -> None:
    names = {job.name for job in scheduler.due_jobs(at(2026, 11, 3, 6, 0))}
    assert names == {"ons_curtailment", "news"}  # news runs every hour
    assert {job.name for job in scheduler.due_jobs(at(2026, 11, 3, 9, 30))} == {"dou_acts"}
    assert {job.name for job in scheduler.due_jobs(at(2026, 11, 3, 8, 30))} == {"ccee_pld"}


def make_job(outcomes: list[Any], retries: int = 2, dataset: str | None = None) -> Job:
    calls = iter(outcomes)

    def run(settings: Any) -> dict[str, Any]:
        outcome = next(calls)
        if isinstance(outcome, Exception):
            raise outcome
        return {"ok": outcome}

    return Job(
        "test", "@hourly", run, retries=retries, retry_delay=timedelta(seconds=7), dataset=dataset
    )


def test_a_job_is_retried_and_stops_at_the_first_success() -> None:
    pauses: list[float] = []
    job = make_job([RuntimeError("boom"), RuntimeError("boom"), True])
    assert scheduler.run_job(job, object(), sleep=pauses.append) is True  # type: ignore[arg-type]
    assert pauses == [7.0, 7.0]


def test_a_job_that_keeps_failing_returns_false_and_records_the_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    recorded: list[tuple[str, str]] = []
    from energy_pipeline import steps

    monkeypatch.setattr(
        steps,
        "record_failure",
        lambda settings, dataset, detail: recorded.append((dataset, detail)),
    )
    job = make_job([RuntimeError("a"), RuntimeError("b")], retries=1, dataset="curtailment")
    assert scheduler.run_job(job, object(), sleep=lambda seconds: None) is False  # type: ignore[arg-type]
    assert recorded == [("curtailment", "test: b")]
