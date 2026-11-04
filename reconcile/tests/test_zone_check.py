from __future__ import annotations

from typing import Any

from reconcile.zone_check import compare, sample, split_by_package


def rec(**kw: Any) -> dict[str, Any]:
    base = {"mmsi": 219000001, "lon": 11.7, "lat": 56.6, "eez_id": "eez:5674", "mpa_ids": [],
            "in_port": False, "dist_eez_boundary_m": 20568.8, "dist_mpa_m": 11851.2}  # fmt: skip
    return {**base, **kw}


def row(eez: str | None = "eez:5674", mpas: list[str] | None = None, port: bool = False,
        boundary: float = 20568.7, mpa: float = 11851.0) -> tuple[Any, ...]:  # fmt: skip
    return (1, eez, mpas or [], port, boundary, mpa)


def test_agreeing_tags_within_tolerance_pass() -> None:
    res = compare([rec()], [row()], tolerance=5.0)
    assert res.examples == []
    assert abs(res.max_boundary_m - 0.1) < 1e-6
    assert abs(res.max_mpa_m - 0.2) < 1e-6


def test_inside_a_protected_area_the_expected_distance_is_zero() -> None:
    res = compare([rec(mpa_ids=["wdpa:1"], dist_mpa_m=0.0)], [row(mpas=["wdpa:1"], mpa=900.0)], tolerance=5.0)
    assert res.examples == []


def test_every_kind_of_disagreement_is_reported() -> None:
    res = compare(
        [rec(eez_id=None), rec(mpa_ids=["wdpa:2"], dist_mpa_m=0.0), rec(in_port=True), rec(dist_eez_boundary_m=20600.0)],
        [row(), row(), row(), row()],
        tolerance=5.0,
    )
    assert (res.eez, res.mpa, res.port) == (1, 1, 1)
    assert len(res.examples) == 5  # the wrong mpa_ids also makes dist_mpa_m disagree
    assert any("dist_eez_boundary_m 20600.0" in e for e in res.examples)


def test_sample_takes_tagged_records_first() -> None:
    records = [rec(mpa_ids=["wdpa:1"])] * 3 + [rec()] * 100
    picked = sample(records, 10, seed=1)
    assert len(picked) == 10
    assert sum(bool(r["mpa_ids"]) for r in picked) == 3


def test_only_records_of_the_active_package_are_checked() -> None:
    records = [rec(zones_version="zones-new"), rec(zones_version="zones-old"), rec(zones_version="zones-old"),
               rec(zones_version=None)]  # fmt: skip
    current, others = split_by_package(records, "zones-new")
    assert current == [records[0]]
    assert others == {"zones-old": 2, "None": 1}
