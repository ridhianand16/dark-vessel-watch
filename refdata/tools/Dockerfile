# GDAL (ogr2ogr) and minisign for building and signing the zone package.
# Built once while online (make refdata-image); the build itself runs with no network.
FROM ghcr.io/osgeo/gdal:ubuntu-small-3.13.1

RUN apt-get update \
 && apt-get install -y --no-install-recommends minisign=0.12-1build1 \
 && rm -rf /var/lib/apt/lists/*
