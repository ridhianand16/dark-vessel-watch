# 0011. The zone package is a signed manifest over per-layer GeoPackages, built locally from pinned sources

Status: accepted (2026-11-03)

## Context

The zones DARK_GAP needs (EEZ polygons and boundary lines, marine protected areas,
ports) come from three external datasets. ADR 0009 pins each one by the SHA-256 of
the file downloaded from a versioned static release, and fixes the area the package
covers. ADR 0010 covers how a package replaces the live zones in PostGIS. Left open:
what a package is on disk, and how the loader knows that one is genuine and complete.

The pipeline is meant to run air-gapped. The package is built where the sources can be
downloaded, then carried to where it is loaded. A checksum next to the data proves
nothing there: anyone who can change a file can change its checksum too. The loader
needs one thing it can trust, set up in advance, that covers everything else.

Two of the sources forbid redistribution or restrict it (WDPA: no redistribution of
the data or derived works; Marine Regions: CC BY-NC-SA). The clipped layers are derived
works, so the package cannot be published or committed either.

## Decision

**Sources.** EEZ polygons and boundary lines come from the Marine Regions versioned
static release, not the WFS. Every source pins a SHA-256 of the downloaded bytes in
`refdata/sources.yaml`. Sources behind a form or a moving URL are placed in
`data/refdata/incoming/` by hand; fetch verifies the hash and prints download
instructions if the file is missing (ADR 0009). The build checks the hashes again
before reading anything.

**Package format.** One GeoPackage per layer plus `manifest.json` (package version,
bbox, margin, port radius, source names, versions and SHA-256, file hashes), signed
once with minisign. The signature covers the manifest; the manifest covers the files.
Only PostGIS reads the package.

The layers are `eez`, `mpa`, `ports` (loaded into `zones`) and `eez_boundaries`. The
manifest also records the WDPA realm filter, the column mapping and the tool image, and
the package version is the first 12 hex digits of a SHA-256 over all of those inputs.
GeoPackage timestamps are pinned, so the same inputs give byte-identical files. A
rebuild gives the same version and the same manifest, which shows nothing else leaked
into the build.

**Verifying.** `refdata verify`, which `refdata load` always runs first, checks in this
order: the minisign signature over `manifest.json` against `refdata/keys/dvw.pub`; that
the signed trusted comment names the package version, and the directory carries that
name; then every listed file's size and SHA-256. A file the manifest does not list also
fails. Nothing from the manifest is used before the signature passes. Verification is
written in Python against the minisign format, so the loading side needs no minisign
binary. A test checks it against signatures made by minisign 0.12.

**Keys.** `refdata/keys/dvw.pub` is committed. The secret key stays outside the
repository (`~/.minisign/dvw.key` by default) and never travels with the package. It
is created without a password so `make refdata` runs unattended. On this laptop the
risk that accepts is that someone with access to the home directory could sign a
package. A build host shared with other people should use a password-protected key
(minisign then prompts during the build).

**Building offline.** `ogr2ogr` and `minisign` run in the `refdata-tools` container,
which has no network interface. The build reads only `data/refdata/incoming/`,
`refdata/area.yaml` and `refdata/package.yaml`.

**No redistribution.** The built package stays in `data/refdata/packages/`, which is
ignored by git, and is never published. Anyone else builds their own package from
their own downloads of the pinned sources and signs it with their own key. The
repository ships the builder, the hashes and the public key.

## Alternatives

- **Checksums only (a SHA256SUMS file).** It catches a corrupted file, but not a
  replaced one, because whoever replaces a file can regenerate the list.
- **Signing each file separately.** That means more signatures, and a package could
  lose a whole layer without any signature failing. One signed manifest also pins the
  set of files.
- **GPG.** It has the same trust model and a much larger tool and keyring to carry
  into an air-gapped site. minisign keys are one line, and the signature format is
  simple enough to verify in a few dozen lines of Python.
- **Sigstore / cosign.** Keyless signing depends on online identity and transparency
  log services, which the air-gapped side cannot reach.
- **One GeoPackage with all layers, or a `pg_dump`.** A single file would work as
  well. One file per layer lets the manifest show feature counts and hashes per layer,
  and the verifier names the layer that changed. A dump would tie the package to one
  PostgreSQL and PostGIS version and could not be inspected with GDAL.
- **Loading the sources straight into PostGIS, no package.** This is simplest, but
  the air-gapped side would then need the raw global files (over 1 GB), and nothing
  would show that what was loaded matches what was reviewed.

## Consequences

- `make refdata` builds, verifies and loads offline. On the pinned day it takes about
  20 s on the laptop (see the README for the breakdown). The first build needs the
  tools image (`make refdata-image`, online once) and a key (`make refdata-key`).
- A change to any input (a new WDPA month, another radius, a different area or tool
  image) gives a new package version. A rebuild from the same inputs reuses the
  existing package if it still verifies, and refuses to overwrite one that does not.
- Rolling back means loading an older package directory that is still on disk. Old
  packages are not removed automatically.
- Losing the secret key means generating a new pair, committing the new public key and
  re-signing. Packages signed with the old key then stop verifying, which is intended.
- Full detail is expensive in the MPA layer: 2.9 million vertices in 2,169 polygons,
  against about 110,000 for the other three layers together. That is fine for PostGIS.
  Before Flink holds zones in broadcast state, it is the first number to look at.
