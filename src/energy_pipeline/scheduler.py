"""A small scheduler that replaces Airflow for a single-machine setup.

    python -m energy_pipeline.scheduler                 # run forever, firing jobs on their cron
    python -m energy_pipeline.scheduler list            # the jobs and their schedules
    python -m energy_pipeline.scheduler run <job> ...   # run jobs once, now

Schedules are in UTC. A job never overlaps itself. A run that is missed because the machine was
off is not replayed (same as the DAGs with `catchup=False`): run it by hand with `run`.
"""

from __future__ import annotations

import argparse
import logging
import sys
import threading
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime

from energy_pipeline import cron
from energy_pipeline.config import Settings
from energy_pipeline.jobs import JOBS, Job

log = logging.getLogger("energy_pipeline.scheduler")


def run_job(job: Job, settings: Settings, sleep: Callable[[float], None] = time.sleep) -> bool:
    """Run a job with its retries. Returns whether it succeeded; a final failure is recorded."""
    for attempt in range(job.retries + 1):
        try:
            started = time.perf_counter()
            summary = job.run(settings)
            log.info("%s done in %.0fs: %s", job.name, time.perf_counter() - started, summary)
            return True
        except Exception as error:
            log.exception("%s failed (attempt %d of %d)", job.name, attempt + 1, job.retries + 1)
            if attempt < job.retries:
                sleep(job.retry_delay.total_seconds())
                continue
            if job.dataset:
                try:
                    from energy_pipeline import steps

                    steps.record_failure(settings, job.dataset, f"{job.name}: {error}"[:2000])
                except Exception:
                    log.exception("could not record the failure of %s", job.name)
    return False


def due_jobs(moment: datetime) -> list[Job]:
    return [job for job in JOBS.values() if cron.matches(job.schedule, moment)]


def serve(settings: Settings, workers: int = 3) -> None:
    for job in JOBS.values():
        cron.parse(job.schedule)  # fail at start-up, not at 3 a.m.
    running: set[str] = set()
    guard = threading.Lock()

    def launch(job: Job) -> None:
        try:
            run_job(job, settings)
        finally:
            with guard:
                running.discard(job.name)

    log.info("scheduler started with %d jobs (UTC)", len(JOBS))
    with ThreadPoolExecutor(max_workers=workers) as pool:
        last_minute = None
        while True:
            now = datetime.now(UTC).replace(second=0, microsecond=0)
            if now != last_minute:
                last_minute = now
                for job in due_jobs(now):
                    with guard:
                        if job.name in running:
                            log.warning("%s is still running; skipping %s", job.name, now)
                            continue
                        running.add(job.name)
                    log.info("starting %s", job.name)
                    pool.submit(launch, job)
            time.sleep(5)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="energy_pipeline.scheduler")
    sub = parser.add_subparsers(dest="command")
    sub.add_parser("list", help="show the jobs")
    run = sub.add_parser("run", help="run jobs once, now")
    run.add_argument("jobs", nargs="+", choices=sorted(JOBS))
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    if args.command == "list":
        for job in JOBS.values():
            print(f"{job.name:<22}{job.schedule:<14}retries={job.retries}")
        return 0
    settings = Settings.from_env()
    if args.command == "run":
        return 0 if all([run_job(JOBS[name], settings) for name in args.jobs]) else 1
    serve(settings)
    return 0


if __name__ == "__main__":
    sys.exit(main())
