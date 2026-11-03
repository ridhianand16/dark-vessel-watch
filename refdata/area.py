"""Compute the zone package area from the pinned AIS day and write refdata/area.yaml.

Usage:
    uv run python -m refdata.area            # pinned day from .env, writes refdata/area.yaml
    uv run python -m refdata.area --csv data/ais/aisdk-2026-10-28.csv --out /tmp/area.yaml

The area is the extent of the feed's valid positions, trimmed by area_trim_fraction
per side and axis, plus a margin of 2 x max_distance_rule_nm (refdata/package.yaml).
The result is committed so building the package stays offline and deterministic.

Invalid positions are dropped first: missing values, latitude outside +-90 or
longitude outside +-180 (which covers AIS "not available", lat 91 / lon 181), and 0,0.
"""

from __future__ import annotations

import argparse
import math
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import polars as pl
import yaml

from common.env import REPO_ROOT, setting

DEFAULT_CONFIG = Path(__file__).with_name("package.yaml")
DEFAULT_OUT = Path(__file__).with_name("area.yaml")
NM_PER_DEGREE_LAT = 60.0
DECIMALS = 4  # about 11 m; the box is rounded outwards to this


@dataclass(frozen=True)
class PackageConfig:
    max_distance_rule_nm: float
    port_radius_nm: float
    area_trim_fraction: float

    @property
    def margin_nm(self) -> float:
        return 2 * self.max_distance_rule_nm


def load_config(path: Path = DEFAULT_CONFIG) -> PackageConfig:
    raw: Any = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"{path} is not a mapping")
    values: dict[str, float] = {}
    for key in ("max_distance_rule_nm", "port_radius_nm", "area_trim_fraction"):
        value = raw.get(key)
        if isinstance(value, bool) or not isinstance(value, int | float) or not math.isfinite(value):
            raise ValueError(f"{path}: {key} must be a number, got {value!r}")
        values[key] = float(value)
    cfg = PackageConfig(**values)
    if cfg.max_distance_rule_nm <= 0 or cfg.port_radius_nm <= 0:
        raise ValueError(f"{path}: distances must be positive")
    if not 0 <= cfg.area_trim_fraction < 0.5:
        raise ValueError(f"{path}: area_trim_fraction must be in [0, 0.5)")
    return cfg


@dataclass(frozen=True)
class BBox:
    min_lon: float
    min_lat: float
    max_lon: float
    max_lat: float

    def as_dict(self) -> dict[str, float]:
        return {"min_lon": self.min_lon, "min_lat": self.min_lat, "max_lon": self.max_lon, "max_lat": self.max_lat}


@dataclass(frozen=True)
class Area:
    rows_total: int
    rows_valid: int
    raw_extent: BBox
    """min/max of every valid position, before trimming (for the record)."""
    data_extent: BBox
    """Trimmed extent of the feed."""
    bbox: BBox
    """data_extent plus the margin: what every layer is clipped to."""


def valid_positions(lf: pl.LazyFrame) -> pl.LazyFrame:
    lat, lon = pl.col("lat"), pl.col("lon")
    return lf.filter(
        lat.is_not_null()
        & lon.is_not_null()
        & lat.is_between(-90.0, 90.0)
        & lon.is_between(-180.0, 180.0)
        & ~((lat == 0.0) & (lon == 0.0))
    )


def expand(extent: BBox, margin_nm: float) -> BBox:
    """Grow extent by margin_nm on every side, rounded outwards.

    A degree of longitude shrinks with cos(latitude), so the longitude margin uses the
    box's highest absolute latitude, where it is widest in degrees.
    """
    dlat = margin_nm / NM_PER_DEGREE_LAT
    min_lat = max(-90.0, extent.min_lat - dlat)
    max_lat = min(90.0, extent.max_lat + dlat)
    far = max(abs(min_lat), abs(max_lat))
    cos_far = math.cos(math.radians(far))
    dlon = 180.0 if cos_far < 1e-9 else margin_nm / (NM_PER_DEGREE_LAT * cos_far)
    scale = 10**DECIMALS
    return BBox(
        min_lon=max(-180.0, math.floor((extent.min_lon - dlon) * scale) / scale),
        min_lat=math.floor(min_lat * scale) / scale,
        max_lon=min(180.0, math.ceil((extent.max_lon + dlon) * scale) / scale),
        max_lat=math.ceil(max_lat * scale) / scale,
    )


def compute_area(positions: pl.LazyFrame, cfg: PackageConfig) -> Area:
    """positions has float columns lat and lon, one row per AIS row."""
    q = cfg.area_trim_fraction
    lat, lon = pl.col("lat"), pl.col("lon")
    total = positions.select(n=pl.len())
    stats = valid_positions(positions).select(
        n=pl.len(),
        raw_min_lat=lat.min(),
        raw_max_lat=lat.max(),
        raw_min_lon=lon.min(),
        raw_max_lon=lon.max(),
        # "lower"/"higher" pick real positions, and with q = 0 they are min/max.
        min_lat=lat.quantile(q, interpolation="lower"),
        max_lat=lat.quantile(1 - q, interpolation="higher"),
        min_lon=lon.quantile(q, interpolation="lower"),
        max_lon=lon.quantile(1 - q, interpolation="higher"),
    )
    rows_total = int(total.collect(engine="streaming").item())
    row = stats.collect(engine="streaming").row(0, named=True)
    if not row["n"]:
        raise ValueError("no valid positions in the input")
    data_extent = BBox(row["min_lon"], row["min_lat"], row["max_lon"], row["max_lat"])
    return Area(
        rows_total=rows_total,
        rows_valid=int(row["n"]),
        raw_extent=BBox(row["raw_min_lon"], row["raw_min_lat"], row["raw_max_lon"], row["raw_max_lat"]),
        data_extent=data_extent,
        bbox=expand(data_extent, cfg.margin_nm),
    )


def scan_ais_csv(path: Path) -> pl.LazyFrame:
    return pl.scan_csv(
        path, schema_overrides={"Latitude": pl.Float64, "Longitude": pl.Float64}
    ).select(lat=pl.col("Latitude"), lon=pl.col("Longitude"))


def render(area: Area, cfg: PackageConfig, ais_day: str, csv_name: str) -> str:
    doc = {
        "ais_day": ais_day,
        "ais_file": csv_name,
        "rows_total": area.rows_total,
        "rows_valid": area.rows_valid,
        "trim_fraction": cfg.area_trim_fraction,
        "margin_nm": cfg.margin_nm,
        "bbox": area.bbox.as_dict(),
        "data_extent": area.data_extent.as_dict(),
        "raw_extent": area.raw_extent.as_dict(),
    }
    header = (
        "# Zone package area. Generated by `make area` (refdata/area.py) from the pinned\n"
        "# AIS day; do not edit by hand. Every layer is clipped to `bbox`, which is\n"
        "# `data_extent` (valid positions, trim_fraction cut per side and axis) plus\n"
        "# margin_nm. raw_extent is the untrimmed min/max, kept for the record.\n"
    )
    return header + yaml.safe_dump(doc, sort_keys=False)


def main(argv: list[str] | None = None) -> int:
    day = setting("AIS_DAY", "")
    data_dir = Path(setting("AIS_DATA_DIR", "./data/ais"))
    if not data_dir.is_absolute():
        data_dir = REPO_ROOT / data_dir
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--day", default=day, help="AIS day (default: AIS_DAY)")
    parser.add_argument("--csv", type=Path, help="AIS CSV (default: AIS_DATA_DIR/aisdk-<day>.csv)")
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    args = parser.parse_args(argv)
    if not args.day:
        parser.error("no AIS day: set AIS_DAY in .env or pass --day")
    csv = args.csv or data_dir / f"aisdk-{args.day}.csv"
    if not csv.exists():
        print(f"error: {csv} not found; run make fetch first", file=sys.stderr)
        return 1

    cfg = load_config(args.config)
    area = compute_area(scan_ais_csv(csv), cfg)
    args.out.write_text(render(area, cfg, args.day, csv.name), encoding="utf-8")
    b = area.bbox
    print(
        f"wrote {args.out}: lon {b.min_lon}..{b.max_lon}, lat {b.min_lat}..{b.max_lat} "
        f"({area.rows_valid:,} of {area.rows_total:,} rows valid, margin {cfg.margin_nm:g} nm)",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
