FROM python:3.12-slim AS builder

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_LINK_MODE=copy

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir uv

WORKDIR /app

# Workspace metadata first for better Docker layer caching.
COPY pyproject.toml uv.lock /app/
COPY b2b_ec_etl/pyproject.toml /app/b2b_ec_etl/pyproject.toml
COPY b2b_ec_warehouse/pyproject.toml /app/b2b_ec_warehouse/pyproject.toml
COPY packages/b2b_ec_utils/pyproject.toml /app/packages/b2b_ec_utils/pyproject.toml
COPY packages/b2b_ec_sources/pyproject.toml /app/packages/b2b_ec_sources/pyproject.toml
COPY deploy_infra/pyproject.toml /app/deploy_infra/pyproject.toml

# Workspace source trees (editable installs reference these paths).
COPY b2b_ec_etl /app/b2b_ec_etl
COPY b2b_ec_warehouse /app/b2b_ec_warehouse
COPY packages /app/packages
COPY deploy_infra /app/deploy_infra

# Sync all workspace packages and groups (includes Dagster webserver CLI deps).
RUN uv sync --frozen --all-packages --all-groups


FROM python:3.12-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/app/.venv/bin:${PATH}" \
    DAGSTER_HOME=/app/.dagster_home \
    PYTHONPATH="/app/b2b_ec_etl/src:/app/packages/b2b_ec_utils/src:/app/packages/b2b_ec_sources/src"

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Runtime virtual environment and uv binary.
COPY --from=builder /app/.venv /app/.venv
COPY --from=builder /usr/local/bin/uv /usr/local/bin/uv

# Runtime code and configs needed by Dagster + dbt assets.
COPY --from=builder /app/b2b_ec_etl /app/b2b_ec_etl
COPY --from=builder /app/b2b_ec_warehouse /app/b2b_ec_warehouse
COPY --from=builder /app/packages /app/packages
COPY --from=builder /app/deploy_infra /app/deploy_infra
COPY pyproject.toml uv.lock /app/
COPY .dbt /app/.dbt
COPY .dagster_home /app/.dagster_home

RUN mkdir -p /app/var/logs /app/var/warehouse

EXPOSE 3000

CMD ["uv", "run", "--no-sync", "dagster", "dev", "-m", "b2b_ec_etl.definitions", "-h", "0.0.0.0", "-p", "3000"]
