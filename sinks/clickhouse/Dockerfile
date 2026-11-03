# ClickHouse sink image, shared by raw-sink and alerts-sink (the argument picks the sink). Build context: repo root (see .dockerignore, an allow-list).
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
