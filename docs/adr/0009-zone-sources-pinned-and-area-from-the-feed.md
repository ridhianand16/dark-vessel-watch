# 0009. Zone sources are pinned files; the package covers the feed's area

Status: accepted (2026-11-03)

## Context

DARK_GAP needs to know whether a vessel was near an EEZ boundary or a marine
protected area, and not in port. That takes three reference datasets: Marine
Regions EEZ polygons and boundary lines, the WDPA marine subset, and NGA's World
Port Index. They are loaded into PostGIS as an offline zone package that must be
provably the same bytes on every machine.

The sources do not make that easy:

- Marine Regions offers a WFS and a versioned static release. The WFS answers
  whatever is current on the server, so the same request can return different
  shapes next month. The static release (World EEZ v12, 2023-10-25) is fixed, but
  its download sits behind a form, so there is no stable URL.
- WDPA is released monthly on a URL that changes with the month. The previous
  month's file returns 404 as soon as the next one appears (checked on
  2026-11-03: `Nov2026` exists, `Oct2026` is gone).
- NGA serves the WPI CSV from a fixed URL but answers scripted clients with HTTP
  503 behind a bot check.
- Licences: WDPA forbids redistribution, Marine Regions is CC BY-NC-SA. The files
  can be fetched by a script but cannot be shipped in this repository.

The datasets are global, but the feed is one day of Danish AIS. Loading the whole
world would cost memory in PostGIS now and in Flink broadcast state later. The
feed's extent is not simply min/max of its positions: on 2026-10-28, 179 fixes
from 17 MMSIs that pass every range check sit as far away as 60 S and 178 E. Some
are bad GPS data; some are real vessels (St Petersburg, Helsinki) picked up far
outside the receiver area.

## Decision

**Every source is a pinned file.** `refdata/sources.yaml` records each source's
version, the SHA-256 of the downloaded bytes, its licence and its attribution.
Everything lives under `data/refdata/incoming/<source>/<version>/`. Then
`make fetch-refdata`:

- verifies files placed there by hand for `manual: true` sources (EEZ and WDPA),
  and prints the download page and exact target path when one is missing;
- downloads WPI, and falls back to the same instructions when NGA refuses;
- fails on a hash mismatch, on an unpinned hash (after printing it), on an HTML
  response, and when an archive lacks the files listed under `members`.

The EEZ boundary lines ship in the same zip as the polygons. They are a separate
source entry that points at it with `archive: eez`, so the zip is verified once.
The WDPA release is chosen with `WDPA_VERSION` in `.env` and must equal the pinned
version while a hash is pinned.

**The package covers the area of the feed, plus a margin.** `make area` scans the
pinned AIS day and drops invalid positions: missing, latitude outside ±90,
longitude outside ±180 (this covers the AIS "not available" values 91 and 181),
and 0,0. It takes the extent after cutting 0.01% of positions from each end of
each axis. It then adds a margin of twice the largest distance rule (DARK_GAP's
10 nm, so 20 nm) and writes `refdata/area.yaml`, which is committed. Every layer
is clipped to that box at full detail. Distances to an EEZ boundary are measured
to the boundary-line layer, never to the edges of clipped polygons, which would
create boundaries that do not exist.

For 2026-10-28 the box is 2.69 to 17.53 E, 53.52 to 59.57 N.

## Alternatives

- **The Marine Regions WFS with a bbox.** No form and only a few MB, but the
  bytes depend on the server's state on the day. A hash cannot pin them, and two
  builds a month apart could disagree.
- **Plain min/max of valid positions.** This was the original plan. On real data
  it gives 60.7 S to 62.4 N and 9.4 W to 178.8 E, which is the whole problem the
  area was meant to avoid.
- **Density filter (0.1° cells seen by at least N vessels).** It keeps every
  well-used cell, but the result depends on N in a way that is hard to reason
  about. With N = 3 the north edge drops to 58.5 N and loses real Skagerrak
  traffic; with N = 2 the box reaches St Petersburg. A quantile trim has one
  parameter whose cost is easy to state: about 1,900 positions per side out of
  18.9 million, all inside the 20 nm margin or beyond the receivers.
- **Whole-world layers.** Simplest to build. They cost far more memory than the
  feed needs, and Flink would carry them in broadcast state on every worker.

## Consequences

- The repository ships the fetcher, `sources.yaml`, `area.yaml` and the
  attribution, never the data. A fresh machine needs three manual downloads
  (EEZ, WDPA, and WPI if NGA refuses the script) before the zone package can be
  built.
- A new WDPA month, EEZ version or WPI download means a new pinned hash and a new
  package version. A different AIS day or region means a new `area.yaml`.
- Vessels in the trimmed tail that are more than 20 nm outside the box get no
  zone context. On the pinned day that is a few hundred fixes, almost all far
  outside the receivers' range.
- `dma_ais` is now pinned as well, since an unpinned hash fails the fetch.
