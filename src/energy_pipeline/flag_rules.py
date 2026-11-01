"""Tariff flag rule inputs per month (``flag_rule_month``), loaded from a versioned seed.

The seed holds NUMERIC FACTS ONLY, read from the CCEE InfoBandeira Tarifaria bulletins
(PLD min/max, GH, GF, GSF, PLD trigger, GFOM, band edges, triggered colour, bulletin number,
date and URL). The bulletin PDFs are never stored or hosted. Before any monetised use of these
facts a legal opinion on the CCEE terms of use is needed (docs/flag-rule-facts.md).

The loader cross-checks every month against ``tariff_flag_monthly`` (ANEEL open data) and stops
the load on any divergence.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from energy_pipeline.ccee import raise_on_errors
from energy_pipeline.config import Settings
from energy_pipeline.quality import (
    CheckResult,
    accepted_values,
    between,
    no_nulls,
    not_empty,
    unique,
)
from energy_pipeline.warehouse import Warehouse

log = logging.getLogger(__name__)

FLAG_RULES_FILE = Path(__file__).parent / "seeds" / "flag_rule_month.csv"
FORECAST_FLAGS = {"Verde", "Amarela", "Vermelha P1", "Vermelha P2"}
TABLE_COLUMNS = [
    "month",
    "rule_version",
    "pld_min_brl_per_mwh",
    "pld_max_brl_per_mwh",
    "gh_mwmed",
    "gf_mwmed",
    "gsf",
    "pld_trigger_brl_per_mwh",
    "gfom_mwmed",
    "green_upper_brl_per_mwh",
    "yellow_upper_brl_per_mwh",
    "red1_upper_brl_per_mwh",
    "flag",
    "bulletin_number",
    "bulletin_url",
    "bulletin_published_on",
    "revised_on",
]
_SCHEMA: dict[str, pl.DataType | type[pl.DataType]] = {
    "month": pl.Date,
    "rule_version": pl.String,
    "pld_min_brl_per_mwh": pl.Float64,
    "pld_max_brl_per_mwh": pl.Float64,
    "gh_mwmed": pl.Float64,
    "gf_mwmed": pl.Float64,
    "gsf": pl.Float64,
    "pld_trigger_brl_per_mwh": pl.Float64,
    "gfom_mwmed": pl.Float64,
    "green_upper_brl_per_mwh": pl.Float64,
    "yellow_upper_brl_per_mwh": pl.Float64,
    "red1_upper_brl_per_mwh": pl.Float64,
    "flag": pl.String,
    "bulletin_number": pl.Int32,
    "bulletin_url": pl.String,
    "bulletin_published_on": pl.Date,
    "announced_on": pl.Date,
    "revised_on": pl.Date,
    "pld_max_table15_brl_per_mwh": pl.Float64,
}
CENT = 0.0051


def read_flag_rules(path: Path = FLAG_RULES_FILE) -> pl.DataFrame:
    return pl.read_csv(path, schema=_SCHEMA).sort("month")


def flag_from_bands(
    trigger: float, green_upper: float, yellow_upper: float, red1_upper: float
) -> str:
    """Colour of the band that holds the trigger (upper edges inclusive, to the cent)."""
    price = round(trigger, 2)
    if price <= green_upper:
        return "Verde"
    if price <= yellow_upper:
        return "Amarela"
    if price <= red1_upper:
        return "Vermelha P1"
    return "Vermelha P2"


def flag_rule_checks(df: pl.DataFrame) -> list[CheckResult]:
    bad_order = df.filter(
        (pl.col("pld_min_brl_per_mwh") > pl.col("pld_max_brl_per_mwh"))
        | (pl.col("green_upper_brl_per_mwh") > pl.col("yellow_upper_brl_per_mwh"))
        | (pl.col("yellow_upper_brl_per_mwh") > pl.col("red1_upper_brl_per_mwh"))
        | (pl.col("red1_upper_brl_per_mwh") > pl.col("pld_max_brl_per_mwh"))
    ).height
    bad_gsf = df.filter(
        ((pl.col("gh_mwmed") / pl.col("gf_mwmed")) - pl.col("gsf")).abs() > CENT
    ).height
    # With GFOM the colour is composed with the thermal dispatch, so only GFOM = 0 months are
    # comparable with the plain band the trigger falls in.
    plain = df.filter(pl.col("gfom_mwmed") == 0)
    mismatched = [
        row["month"].isoformat()
        for row in plain.iter_rows(named=True)
        if flag_from_bands(
            row["pld_trigger_brl_per_mwh"],
            row["green_upper_brl_per_mwh"],
            row["yellow_upper_brl_per_mwh"],
            row["red1_upper_brl_per_mwh"],
        )
        != row["flag"]
        and row["revised_on"] is None
    ]
    return [
        not_empty(df),
        no_nulls(
            df,
            [
                c
                for c in TABLE_COLUMNS
                if not c.startswith(("bulletin_url", "bulletin_pub", "revised"))
            ],
        ),
        unique(df, ["month"]),
        accepted_values(df, "flag", FORECAST_FLAGS),
        between(df, "gsf", 0.0, 3.0),
        CheckResult("band_order", bad_order == 0, f"{bad_order} months with unordered bands"),
        CheckResult(
            "gsf_is_gh_over_gf", bad_gsf == 0, f"{bad_gsf} months differ by more than 0.005"
        ),
        CheckResult("trigger_in_flag_band", not mismatched, f"months: {mismatched}"),
    ]


def cross_check(seed: pl.DataFrame, aneel: pl.DataFrame) -> CheckResult:
    """Every seed month must carry the same colour as ``tariff_flag_monthly``."""
    joined = seed.select("month", "flag").join(
        aneel.select("month", pl.col("flag").alias("aneel_flag")), on="month", how="left"
    )
    wrong = joined.filter(pl.col("aneel_flag").is_null() | (pl.col("flag") != pl.col("aneel_flag")))
    return CheckResult(
        "matches_tariff_flag_monthly",
        wrong.height == 0,
        f"{wrong.height} divergent months: {wrong['month'].dt.strftime('%Y-%m').to_list()}",
    )


def run(settings: Settings) -> dict[str, object]:
    started = datetime.now(UTC)
    seed = read_flag_rules()
    warehouse = Warehouse(settings.warehouse_dsn)
    with warehouse.session() as session:
        rows = session.fetchall("SELECT month, flag FROM tariff_flag_monthly")
    aneel = pl.DataFrame(rows, schema={"month": pl.Date, "flag": pl.String}, orient="row")
    warnings = raise_on_errors([*flag_rule_checks(seed), cross_check(seed, aneel)])
    with warehouse.session() as session:
        session.lock("flag_rule_month")
        session.upsert("flag_rule_month", seed.select(TABLE_COLUMNS), keys=["month"])
    warehouse.record_load(
        dataset="flag_rule_month",
        status="success",
        started_at=started,
        finished_at=datetime.now(UTC),
        rows=seed.height,
    )
    return {"rows": seed.height, "warnings": warnings}
