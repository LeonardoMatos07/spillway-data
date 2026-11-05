"""The scheduled jobs: the same work as the Airflow DAGs, as plain functions.

Every job takes the settings and returns a small summary. The registry below carries the cron
schedule (UTC, like the DAGs), the number of retries and the pause between them.
"""

from __future__ import annotations

import logging
import os
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Any

from energy_pipeline.config import Settings
from energy_pipeline.period import Period, trailing_periods
from energy_pipeline.sources import PIPELINES

log = logging.getLogger(__name__)

Summary = dict[str, Any]


def _window_months() -> int:
    return int(os.getenv("REPROCESS_MONTHS", "3"))


def run_pipeline(
    pipeline: str, settings: Settings, *, window_months: int, publishes: bool
) -> Summary:
    """Extract, transform, validate and record each source for the trailing months; then publish
    to the warehouse, tell the portal and (curtailment) build and announce the monthly report."""
    from energy_pipeline import steps
    from energy_pipeline.__main__ import run_source

    now = datetime.now(UTC)
    periods = trailing_periods(Period.from_datetime(now).previous(), window_months)
    steps.prepare_infrastructure(settings)
    loaded = 0
    for period in periods:
        for name, source in PIPELINES[pipeline].items():
            if period in source.periods_to_process([period]):
                run_source(name, period, settings)
                loaded += 1
    published = 0
    if publishes:
        for period in periods:
            steps.run_publish(period, settings, pipeline)
            published += 1
        steps.notify_portal(settings)
        if pipeline == "curtailment":
            labels = [str(period) for period in periods]
            steps.publish_reports(labels, settings)
            steps.announce_report(labels, now, False, settings)
    return {"pipeline": pipeline, "periods": len(periods), "loaded": loaded, "published": published}


def _pipeline_job(
    pipeline: str, *, windowed: bool, publishes: bool
) -> Callable[[Settings], Summary]:
    def job(settings: Settings) -> Summary:
        return run_pipeline(
            pipeline,
            settings,
            window_months=_window_months() if windowed else 1,
            publishes=publishes,
        )

    return job


def ccee_pld(settings: Settings) -> Summary:
    from energy_pipeline import ccee, market_extra, notify

    loaded = ccee.run_all(settings, False)
    market = market_extra.run_free_market(settings)
    notify.request_revalidation(settings, ["market"])
    return {"pld_sets": len(loaded), "free_market": market}


def aneel_reliability(settings: Settings) -> Summary:
    from energy_pipeline import aneel_reliability as module
    from energy_pipeline import notify

    result = module.run(settings)
    notify.request_revalidation(settings, ["reliability"])
    return result


def aneel_tariff_flags(settings: Settings) -> Summary:
    from energy_pipeline import market_extra, notify

    result = market_extra.run_flags(settings)
    notify.request_revalidation(settings, ["market"])
    return result


def plant_location(settings: Settings) -> Summary:
    from energy_pipeline import notify
    from energy_pipeline import plant_location as module

    result = module.run(settings)
    notify.request_revalidation(settings, ["curtailment", "plants"])
    return result


def news(settings: Settings) -> Summary:
    from energy_pipeline.news.run import run_news

    results = run_news(settings)
    if results and all(r.error for r in results):
        raise RuntimeError("every news source failed: " + "; ".join(r.error or "" for r in results))
    return {"sources": len(results), "stored": sum(r.stored for r in results)}


def dou_acts(settings: Settings) -> Summary:
    from energy_pipeline.news.dou import run_dou

    result = run_dou(settings, date.today() - timedelta(days=1))
    return {"day": str(result.day), "stored": result.stored, "skipped": result.skipped}


@dataclass(frozen=True)
class Job:
    name: str
    schedule: str
    run: Callable[[Settings], Summary]
    retries: int = 2
    retry_delay: timedelta = timedelta(minutes=5)
    # A failed run is recorded in `load_run` under this dataset name (pipelines only).
    dataset: str | None = None


def _pipeline(name: str, pipeline: str, schedule: str, *, windowed: bool, publishes: bool) -> Job:
    return Job(
        name,
        schedule,
        _pipeline_job(pipeline, windowed=windowed, publishes=publishes),
        dataset=pipeline,
    )


_TEN_MINUTES = timedelta(minutes=10)

JOBS: dict[str, Job] = {
    job.name: job
    for job in (
        _pipeline("ons_curtailment", "curtailment", "0 6 3 * *", windowed=True, publishes=True),
        _pipeline("ons_system", "system", "0 7 3 * *", windowed=True, publishes=True),
        _pipeline("ons_hydrology", "hydrology", "0 8 3 * *", windowed=True, publishes=True),
        _pipeline("ons_grid", "grid", "0 0 1 * *", windowed=False, publishes=False),
        Job("ccee_pld", "30 8 * * *", ccee_pld, retry_delay=_TEN_MINUTES),
        Job("aneel_reliability", "0 9 6 * *", aneel_reliability, retry_delay=_TEN_MINUTES),
        Job("aneel_tariff_flags", "30 9 6 * *", aneel_tariff_flags, retry_delay=_TEN_MINUTES),
        Job("plant_location", "0 10 * * 1", plant_location, retries=1, retry_delay=_TEN_MINUTES),
        Job("news", "@hourly", news),
        Job("dou_acts", "30 9 * * *", dou_acts),
    )
}
