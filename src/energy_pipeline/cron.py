"""Five-field cron expressions, enough for the schedules of the pipelines.

Supports `*`, numbers, comma lists, ranges (`1-5`), steps (`*/15`, `1-10/2`) and the aliases
`@hourly`, `@daily` and `@monthly`. Day of week is 0-6 with 0 = Sunday (7 is accepted too).
When both day of month and day of week are restricted, either one matching is enough (as cron).
"""

from __future__ import annotations

from datetime import datetime

ALIASES = {
    "@hourly": "0 * * * *",
    "@daily": "0 0 * * *",
    "@monthly": "0 0 1 * *",
}
RANGES = ((0, 59), (0, 23), (1, 31), (1, 12), (0, 7))


def _expand(field: str, low: int, high: int) -> set[int]:
    values: set[int] = set()
    for part in field.split(","):
        step = 1
        if "/" in part:
            part, raw_step = part.split("/", 1)
            step = int(raw_step)
            if step < 1:
                raise ValueError(f"invalid step in {field!r}")
        if part == "*":
            start, end = low, high
        elif "-" in part:
            raw_start, raw_end = part.split("-", 1)
            start, end = int(raw_start), int(raw_end)
        else:
            start = end = int(part)
        if start < low or end > high or start > end:
            raise ValueError(f"{field!r} is outside {low}-{high}")
        values.update(range(start, end + 1, step))
    return values


def parse(expression: str) -> tuple[set[int], set[int], set[int], set[int], set[int], bool, bool]:
    fields = ALIASES.get(expression, expression).split()
    if len(fields) != 5:
        raise ValueError(f"expected 5 fields in {expression!r}")
    minute, hour, day, month, weekday = (
        _expand(field, low, high) for field, (low, high) in zip(fields, RANGES, strict=True)
    )
    weekday = {value % 7 for value in weekday}
    return minute, hour, day, month, weekday, fields[2] != "*", fields[4] != "*"


def matches(expression: str, moment: datetime) -> bool:
    """Whether `moment` (to the minute) is one of the times the expression fires."""
    minute, hour, day, month, weekday, day_restricted, weekday_restricted = parse(expression)
    if moment.minute not in minute or moment.hour not in hour or moment.month not in month:
        return False
    day_ok = moment.day in day
    weekday_ok = (moment.weekday() + 1) % 7 in weekday  # Python: Monday = 0
    if day_restricted and weekday_restricted:
        return day_ok or weekday_ok
    return day_ok and weekday_ok
