"""Approximate position of each wind and solar plant: the substation it connects to.

The ONS curtailment files give a connection point text ("S.JOAO PIAUI - 500 kV (A)" or
"PIRAPORA 2345kVA", where the voltage is glued to the name) and no coordinates. The substation
register has the coordinates. A plant gets the coordinate of the substation that has the same
normalised name **in the same state** (and the same voltage when the text has one). A name alone is
never matched, and a name that points to two different places is not guessed: those plants get no
position and the portal lists them as "sem localização".

python -m energy_pipeline.plant_location
"""

from __future__ import annotations

import json
import logging
import re
import unicodedata
from collections import defaultdict
from typing import Any

import polars as pl

from energy_pipeline.config import Settings
from energy_pipeline.warehouse import Warehouse

log = logging.getLogger(__name__)

SOURCE = "ons-substation"
PRECISION = "substation"
# Below this share of matched plants something is wrong with the data: keep the previous result.
MIN_MATCH_RATE = 0.70

_BAR = re.compile(r"\s*\([A-Z]\)\s*$")
_VOLTS = re.compile(r"-?\s*(\d{2,3})\s*KV\s*$")
_VOLTS_GLUED = re.compile(r"(\d{2,3})KVA$")


def normalize_name(value: str) -> str:
    """Upper case, no accents, only letters and digits: 'S.JOAO PIAUI' == 's joao piauí'."""
    text = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode().upper()
    return re.sub(r"[^A-Z0-9]", "", text)


def parse_connection_point(text: str) -> tuple[str, int | None]:
    """Substation name and voltage (kV) of a connection point text; the voltage may be missing."""
    value = _BAR.sub("", text.upper())
    if (found := _VOLTS.search(value)) is not None:
        return value[: found.start()].strip(" -"), int(found.group(1))
    if (found := _VOLTS_GLUED.search(value)) is not None:
        return value[: found.start()].strip(" -"), int(found.group(1))
    return value.strip(" -"), None


def match_plants(plants: pl.DataFrame, substations: pl.DataFrame) -> pl.DataFrame:
    """One row per plant that found its substation: plant_id, latitude, longitude, matched_on.

    ``plants``: id, state, connection_point. ``substations``: station_id, name, state,
    voltage_kv, latitude, longitude (current versions with a coordinate)."""
    index: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in substations.iter_rows(named=True):
        if row["name"] is None or row["latitude"] is None or row["longitude"] is None:
            continue
        index[(normalize_name(row["name"]), row["state"])].append(row)

    matched: list[dict[str, Any]] = []
    for plant in plants.iter_rows(named=True):
        if not plant["connection_point"]:
            continue
        name, volts = parse_connection_point(plant["connection_point"])
        candidates = index.get((normalize_name(name), plant["state"]), [])
        if volts is not None:
            same_volts = [
                c for c in candidates if c["voltage_kv"] and round(c["voltage_kv"]) == volts
            ]
            candidates = same_volts or candidates
        places = {(c["latitude"], c["longitude"]) for c in candidates}
        if len(places) != 1:  # no substation, or the name points to two places: do not guess
            continue
        best = sorted(candidates, key=lambda c: c["station_id"])[0]
        matched.append(
            {
                "plant_id": plant["id"],
                "latitude": float(best["latitude"]),
                "longitude": float(best["longitude"]),
                "matched_on": best["station_id"],
            }
        )
    return pl.DataFrame(
        matched,
        schema={
            "plant_id": pl.Utf8,
            "latitude": pl.Float64,
            "longitude": pl.Float64,
            "matched_on": pl.Utf8,
        },
    )


def run(settings: Settings) -> dict[str, object]:
    warehouse = Warehouse(settings.warehouse_dsn)
    with warehouse.session() as session:
        session.lock("plant_location")
        plants = pl.DataFrame(
            session.fetchall("SELECT id, state, connection_point FROM plant WHERE kind = 'plant'"),
            schema={"id": pl.Utf8, "state": pl.Utf8, "connection_point": pl.Utf8},
            orient="row",
        )
        substations = pl.DataFrame(
            session.fetchall(
                "SELECT station_id, name, state, voltage_kv, latitude, longitude FROM substation "
                "WHERE valid_to IS NULL AND latitude IS NOT NULL AND longitude IS NOT NULL"
            ),
            schema={
                "station_id": pl.Utf8,
                "name": pl.Utf8,
                "state": pl.Utf8,
                "voltage_kv": pl.Float64,
                "latitude": pl.Float64,
                "longitude": pl.Float64,
            },
            orient="row",
        )
        official = {
            row[0]
            for row in session.fetchall(
                "SELECT plant_id FROM plant_location WHERE location_precision = 'official'"
            )
        }
        located = match_plants(plants.filter(~pl.col("id").is_in(list(official))), substations)
        rate = located.height / max(plants.height - len(official), 1)
        if rate < MIN_MATCH_RATE:
            raise RuntimeError(
                f"only {rate:.0%} of the plants matched a substation (minimum "
                f"{MIN_MATCH_RATE:.0%}): the previous positions were kept"
            )
        session.execute(
            "DELETE FROM plant_location WHERE location_precision = %s AND source = %s",
            (PRECISION, SOURCE),
        )
        if located.height:
            session.upsert(
                "plant_location",
                located.with_columns(
                    pl.lit(PRECISION).alias("location_precision"), pl.lit(SOURCE).alias("source")
                ),
                keys=["plant_id"],
            )
    result: dict[str, object] = {
        "plants": plants.height,
        "official": len(official),
        "located": located.height,
        "without_location": plants.height - len(official) - located.height,
        "match_rate": round(rate, 4),
    }
    log.info("plant_location: %s", result)
    return result


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    print(json.dumps(run(Settings.from_env()), indent=2))


if __name__ == "__main__":
    main()
