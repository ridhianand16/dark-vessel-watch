# Jobs image for Kubernetes (deploy/helm): schema migrations, zone package load, the
# replayer and the reconciliation job (which reads topic names from stream/). Not used by Compose, where these run on the host
# with uv. Build context: repo root; jobs.Dockerfile.dockerignore (next to this file)
# replaces the root .dockerignore for this build.
#
#   docker build -f deploy/docker/jobs.Dockerfile -t dvw-jobs:0.1.0 .
#
# Pure Python: zone packages are read with sqlite3 and verified with the cryptography
# package, so no GDAL or minisign binary is needed (building a package still is, and
# stays offline on an operator's machine). Dependencies come from uv.lock at build time.
FROM python:3.12.16-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_PYTHON_DOWNLOADS=never \
    UV_COMPILE_BYTECODE=1 \
    PATH=/opt/venv/bin:$PATH \
    REFDATA_DIR=/data/refdata

RUN pip install --no-cache-dir uv==0.13.6

WORKDIR /app
COPY pyproject.toml uv.lock .python-version ./
RUN uv sync --frozen --no-dev --no-cache --no-install-project

COPY common/ common/
COPY sql/ sql/
COPY ingest/ ingest/
COPY refdata/ refdata/
COPY reconcile/ reconcile/
COPY stream/ stream/

USER nobody
ENTRYPOINT ["python", "-m"]
CMD ["sql.migrate", "--help"]
