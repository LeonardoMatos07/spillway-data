"""Weekly recalculation of the approximate position of each plant (its substation)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from airflow.sdk import dag, task


@dag(
    dag_id="plant_location_pipeline",
    description="Match each plant's connection point with a substation of the grid register",
    schedule="0 10 * * 1",
    start_date=datetime(2026, 11, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "data-engineering", "retries": 1, "retry_delay": timedelta(minutes=10)},
    tags=["etl", "ons", "maps"],
    doc_md=__doc__,
)
def plant_location_pipeline() -> None:
    @task
    def match() -> dict[str, Any]:
        from energy_pipeline import plant_location
        from energy_pipeline.config import Settings

        return plant_location.run(Settings.from_env())

    @task
    def notify_portal() -> dict[str, Any]:
        from energy_pipeline import notify
        from energy_pipeline.config import Settings

        return notify.request_revalidation(Settings.from_env(), ["curtailment", "plants"])

    match() >> notify_portal()


plant_location_pipeline()
