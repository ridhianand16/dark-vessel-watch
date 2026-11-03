from __future__ import annotations

import math
from pathlib import Path

import polars as pl
import pytest

from refdata.area import BBox, PackageConfig, compute_area, expand, load_config, scan_ais_csv

NO_TRIM = PackageConfig(max_distance_rule_nm=10, port_radius_nm=2, area_trim_fraction=0)


def frame(points: list[tuple[float | None, float | None]]) -> pl.LazyFrame:
    return pl.LazyFrame({"lat": [p[0] for p in points], "lon": [p[1] for p in points]}, schema={"lat": pl.Float64, "lon": pl.Float64})


def test_invalid_positions_are_dropped_before_min_max() -> None:
    area = compute_area(
        frame([(55.0, 10.0), (56.0, 12.0), (91.0, 181.0), (91.0, 10.0), (55.5, 181.0), (0.0, 0.0), (None, 10.0), (-91.0, 5.0)]),
        NO_TRIM,
    )
    assert area.rows_total == 8
    assert area.rows_valid == 2
    assert area.data_extent == BBox(min_lon=10.0, min_lat=55.0, max_lon=12.0, max_lat=56.0)


def test_zero_latitude_alone_is_valid() -> None:
    area = compute_area(frame([(0.0, 5.0), (1.0, 6.0)]), NO_TRIM)
    assert area.rows_valid == 2


def test_margin_is_twice_the_largest_rule_and_wider_in_longitude_up_north() -> None:
    box = expand(BBox(min_lon=10.0, min_lat=55.0, max_lon=12.0, max_lat=56.0), margin_nm=20)
    assert box.min_lat == pytest.approx(55.0 - 20 / 60, abs=1e-4)
    assert box.max_lat == pytest.approx(56.0 + 20 / 60, abs=1e-4)
    dlon = 20 / (60 * math.cos(math.radians(56.0 + 20 / 60)))
    assert box.min_lon == pytest.approx(10.0 - dlon, abs=1e-4)
    assert box.max_lon == pytest.approx(12.0 + dlon, abs=1e-4)
    # rounded outwards, so the box never shrinks below the exact margin
    assert box.min_lat <= 55.0 - 20 / 60 and box.max_lon >= 12.0 + dlon


def test_expand_clamps_at_the_poles_and_antimeridian() -> None:
    box = expand(BBox(min_lon=-179.9, min_lat=-89.9, max_lon=179.9, max_lat=89.9), margin_nm=20)
    assert box == BBox(min_lon=-180.0, min_lat=-90.0, max_lon=180.0, max_lat=90.0)


def test_trim_drops_isolated_outliers() -> None:
    points: list[tuple[float | None, float | None]] = [(55.0 + i / 1000, 10.0 + i / 1000) for i in range(1000)]
    points.append((-60.0, 178.0))  # a bogus fix on the other side of the world
    cfg = PackageConfig(max_distance_rule_nm=10, port_radius_nm=2, area_trim_fraction=0.001)
    area = compute_area(frame(points), cfg)
    assert area.raw_extent.min_lat == -60.0
    assert area.data_extent.min_lat == 55.0
    assert area.data_extent.max_lon == pytest.approx(10.999)


def test_scan_reads_dma_columns(tmp_path: Path) -> None:
    csv = tmp_path / "ais.csv"
    csv.write_text(
        "# Timestamp,MMSI,Latitude,Longitude\n"
        "28/10/2026 00:00:00,219005886,56.794840,8.863737\n"
        "28/10/2026 00:00:01,219026147,91.000000,181.000000\n",
        encoding="utf-8",
    )
    area = compute_area(scan_ais_csv(csv), NO_TRIM)
    assert (area.rows_total, area.rows_valid) == (2, 1)


def test_config_rejects_nonsense(tmp_path: Path) -> None:
    path = tmp_path / "package.yaml"
    path.write_text("max_distance_rule_nm: 10\nport_radius_nm: 2\narea_trim_fraction: 0.6\n", encoding="utf-8")
    with pytest.raises(ValueError, match="area_trim_fraction"):
        load_config(path)
    path.write_text("max_distance_rule_nm: ten\nport_radius_nm: 2\narea_trim_fraction: 0\n", encoding="utf-8")
    with pytest.raises(ValueError, match="max_distance_rule_nm"):
        load_config(path)


def test_committed_config_gives_a_20_nm_margin() -> None:
    cfg = load_config()
    assert cfg.margin_nm == 20
    assert cfg.port_radius_nm == 2
