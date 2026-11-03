from __future__ import annotations

from typing import Any

from reconcile.flink_check import Tally, render, tally
from reconcile.run_check import Produced

RUN = "20261103T172423Z-8b1198"
PRODUCED = [Produced(0, 100, 109, 10), Produced(3, 50, 52, 3)]


TAGS = {"zones_version": "zones-ee4167a002e7", "dist_eez_boundary_m": 9046.6}


def rec(partition: int, offset: int, run_id: str | None = RUN, **extra: Any) -> dict[str, Any]:
    return {"src_partition": partition, "src_offset": offset, "run_id": run_id, **TAGS, **extra}


def count(partition: int, n: int, run_id: str = RUN) -> dict[str, Any]:
    return {"run_id": run_id, "src_partition": partition, "count": n}


def test_balanced_run_is_ok() -> None:
    enriched = [rec(0, o) for o in range(100, 106)] + [rec(3, 50)]
    dlq = [rec(0, 106, reason="null_island"), rec(3, 51, None, reason="decode_error")]
    counts = [count(0, 2), count(0, 1), count(3, 1)]  # several records per window are summed
    tallies, reasons = tally(RUN, PRODUCED, enriched, dlq, counts)
    assert [t.ok for t in tallies] == [True, True]
    assert (tallies[0].enriched, tallies[0].dlq, tallies[0].duplicates) == (6, 1, 3)
    assert reasons == {"null_island": 1, "decode_error": 1}


def test_missing_is_reported() -> None:
    tallies, _ = tally(RUN, PRODUCED, [rec(0, o) for o in range(100, 108)], [], [count(3, 3)])
    assert not tallies[0].ok
    assert tallies[0].note == "missing 2"
    assert tallies[1].ok


def test_offset_twice_fails_even_when_the_total_matches() -> None:
    # 100 written twice and 109 never: the count balances, the offsets do not.
    enriched = [rec(0, o) for o in range(100, 109)] + [rec(0, 100)]
    tallies, _ = tally(RUN, PRODUCED[:1], enriched, [], [])
    assert tallies[0].accounted == 10
    assert not tallies[0].ok
    assert "1 offsets twice" in tallies[0].note


def test_offset_in_enriched_and_dlq_counts_as_twice() -> None:
    tallies, _ = tally(RUN, PRODUCED[1:], [rec(3, 50), rec(3, 51)], [rec(3, 51, reason="bad_mmsi")], [])
    assert tallies[0].repeated == 1


def test_records_outside_the_range_or_from_other_runs_counts_are_ignored() -> None:
    enriched = [rec(0, 99), rec(0, 110), rec(1, 100), rec(3, 50)]
    counts = [count(3, 2, run_id="other-run"), count(3, 2)]
    tallies, _ = tally(RUN, PRODUCED[1:], enriched, [], counts)
    assert (tallies[0].enriched, tallies[0].duplicates) == (1, 2)
    assert tallies[0].ok


def test_foreign_run_id_in_the_range_fails() -> None:
    tallies, _ = tally(RUN, PRODUCED[1:], [rec(3, 50), rec(3, 51), rec(3, 52, run_id="other")], [], [])
    assert tallies[0].foreign == 1
    assert not tallies[0].ok


def test_bitmap_covers_the_last_offset() -> None:
    t = Tally(Produced(0, 0, 7, 8))
    assert all(t.mark(o) for o in range(8))
    assert not t.mark(7)


def test_render_shows_status_and_reasons() -> None:
    tallies, reasons = tally(RUN, PRODUCED[1:], [rec(3, 50)], [rec(3, 51, reason="bad_mmsi")], [count(3, 1)])
    out = render(RUN, tallies, reasons)
    assert "OK" in out
    assert "dlq reasons: bad_mmsi 1" in out


def test_enriched_record_without_zone_tags_fails() -> None:
    enriched = [rec(3, 50), rec(3, 51, zones_version=None), rec(3, 52, dist_eez_boundary_m=None)]
    tallies, _ = tally(RUN, PRODUCED[1:], enriched, [], [])
    assert tallies[0].accounted == 3
    assert not tallies[0].ok
    assert tallies[0].note == "2 without zone tags"
