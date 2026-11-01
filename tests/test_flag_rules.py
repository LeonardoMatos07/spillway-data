from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from energy_pipeline import flag_rules
from energy_pipeline.ccee import raise_on_errors
from energy_pipeline.quality import DataQualityError


def test_seed_is_consistent_and_covers_the_bulletins_read():
    seed = flag_rules.read_flag_rules()

    assert seed.height >= 37
    assert seed["month"].min() == date(2022, 8, 1)
    assert seed["month"].max() == date(2026, 10, 1)
    assert raise_on_errors(flag_rules.flag_rule_checks(seed)) == []
    assert set(seed["rule_version"].to_list()) == {"B", "C"}
    # September 2024 was revised on 2024-09-04 (ANEEL dispatch 2.668/2024)
    revised = seed.filter(pl.col("revised_on").is_not_null())
    assert revised["month"].to_list() == [date(2024, 9, 1)]


def test_the_seed_holds_only_numeric_facts_and_pointers():
    seed = flag_rules.read_flag_rules()
    assert seed.columns == [*flag_rules._SCHEMA]
    text_columns = {name for name, dtype in seed.schema.items() if dtype == pl.String}
    assert text_columns == {"rule_version", "flag", "bulletin_url"}
    urls = seed["bulletin_url"].drop_nulls().to_list()
    assert all(url.startswith("https://www.ccee.org.br/documents/") for url in urls)


def test_flag_from_bands_edges_are_inclusive():
    assert flag_rules.flag_from_bands(98.14, 98.14, 246.40, 339.47) == "Verde"
    assert flag_rules.flag_from_bands(98.15, 98.14, 246.40, 339.47) == "Amarela"
    assert flag_rules.flag_from_bands(339.47, 98.14, 246.40, 339.47) == "Vermelha P1"
    assert flag_rules.flag_from_bands(339.48, 98.14, 246.40, 339.47) == "Vermelha P2"


def test_a_colour_that_disagrees_with_the_trigger_blocks_the_load():
    seed = flag_rules.read_flag_rules().with_columns(
        pl.when(pl.col("month") == date(2026, 8, 1))
        .then(pl.lit("Verde"))
        .otherwise(pl.col("flag"))
        .alias("flag")
    )
    with pytest.raises(DataQualityError):
        raise_on_errors(flag_rules.flag_rule_checks(seed))


def test_unordered_bands_block_the_load():
    seed = flag_rules.read_flag_rules().with_columns(
        pl.when(pl.col("month") == date(2026, 8, 1))
        .then(pl.lit(100.0))
        .otherwise(pl.col("red1_upper_brl_per_mwh"))
        .alias("red1_upper_brl_per_mwh")
    )
    with pytest.raises(DataQualityError):
        raise_on_errors(flag_rules.flag_rule_checks(seed))


def test_cross_check_against_aneel_series():
    seed = flag_rules.read_flag_rules()
    aneel = seed.select("month", "flag")
    assert flag_rules.cross_check(seed, aneel).passed
    changed = aneel.with_columns(
        pl.when(pl.col("month") == date(2024, 9, 1))
        .then(pl.lit("Amarela"))
        .otherwise(pl.col("flag"))
        .alias("flag")
    )
    result = flag_rules.cross_check(seed, changed)
    assert not result.passed and "2024-09" in result.details
    assert not flag_rules.cross_check(seed, aneel.head(5)).passed
