import polars as pl
import pytest

from energy_pipeline import plant_location as pl_mod


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("S.JOAO PIAUI - 500 kV (A)", ("S.JOAO PIAUI", 500)),
        ("PIRAPORA 2345kVA", ("PIRAPORA 2", 345)),
        ("RIO BRILHANTE230kVA", ("RIO BRILHANTE", 230)),
        ("IGAPORA II - 230 kV (B)", ("IGAPORA II", 230)),
        ("ICO230kVA", ("ICO", 230)),
        ("SEM TENSAO", ("SEM TENSAO", None)),
    ],
)
def test_connection_point_text_gives_name_and_voltage(text, expected):
    assert pl_mod.parse_connection_point(text) == expected


def test_names_are_compared_without_accents_spaces_or_punctuation():
    assert pl_mod.normalize_name("S.JOAO PIAUÍ") == pl_mod.normalize_name("s joao piaui")


SUBSTATIONS = pl.DataFrame(
    {
        "station_id": ["A5", "A2", "B2", "R1", "R2", "C2"],
        "name": ["ALFA", "ALFA", "BETA", "REPETIDA", "REPETIDA", "GAMA"],
        "state": ["BA", "BA", "PE", "SP", "SP", "CE"],
        "voltage_kv": [500.0, 230.0, 230.0, 138.0, 138.0, 230.0],
        "latitude": [-10.0, -10.0, -8.0, -23.0, -22.0, None],
        "longitude": [-40.0, -40.0, -37.0, -46.0, -47.0, None],
    }
)


def plants(*rows):
    return pl.DataFrame(
        rows, schema={"id": pl.Utf8, "state": pl.Utf8, "connection_point": pl.Utf8}, orient="row"
    )


def test_a_plant_takes_the_coordinate_of_the_substation_of_the_same_name_and_state():
    out = pl_mod.match_plants(
        plants(("P1", "BA", "ALFA - 500 kV (A)"), ("P2", "PE", "BETA230kVA")), SUBSTATIONS
    )
    rows = {r["plant_id"]: r for r in out.iter_rows(named=True)}
    assert rows["P1"]["latitude"] == -10.0 and rows["P1"]["matched_on"] == "A5"
    assert rows["P2"]["longitude"] == -37.0


def test_voltage_picks_among_levels_of_the_same_place():
    out = pl_mod.match_plants(plants(("P1", "BA", "ALFA - 230 kV (A)")), SUBSTATIONS)
    assert out["matched_on"].to_list() == ["A2"]


def test_a_name_alone_is_never_matched():
    out = pl_mod.match_plants(plants(("P1", "PE", "ALFA - 500 kV (A)")), SUBSTATIONS)
    assert out.height == 0  # ALFA exists, but in BA


def test_a_name_that_points_to_two_places_is_not_guessed():
    out = pl_mod.match_plants(plants(("P1", "SP", "REPETIDA - 138 kV (A)")), SUBSTATIONS)
    assert out.height == 0


def test_no_coordinate_no_connection_point_and_unknown_names_stay_unlocated():
    out = pl_mod.match_plants(
        plants(("P1", "CE", "GAMA230kVA"), ("P2", "BA", None), ("P3", "BA", "NAOEXISTE")),
        SUBSTATIONS,
    )
    assert out.height == 0
