# Sink image, shared by raw-sink, tracks-alerts-sink (the arguments pick the ClickHouse sinks) and
# vessels-sink (Compose overrides the entrypoint to python -m sinks.postgis_vessels). Build context: repo root (see .dockerignore, an allow-list).
# Dependencies come from uv.lock at build time; nothing is downloaded at runtime.
FROM python:3.12.16-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_PYTHON_DOWNLOADS=never \
    UV_COMPILE_BYTECODE=1 \
    PATH=/opt/venv/bin:$PATH

RUN pip install --no-cache-dir uv==0.13.6

WORKDIR /app
COPY pyproject.toml uv.lock .python-version ./
RUN uv sync --frozen --no-dev --no-cache --no-install-project

COPY common/ common/
COPY sinks/ sinks/

USER nobody
ENTRYPOINT ["python", "-m", "sinks.clickhouse"]
CMD ["raw"]
