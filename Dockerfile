# syntax=docker/dockerfile:1
# Trader platform image (docs/runbook.md). Build:  docker build -t trader .
# The base image is a build arg so a registry mirror can stand in for Docker Hub:
#   docker build --build-arg PYTHON_IMAGE=mirror.gcr.io/library/python:3.12-slim-bookworm -t trader .
ARG PYTHON_IMAGE=python:3.12-slim-bookworm

# ---------------------------------------------------------------- build: locked dependencies into /app/.venv
FROM ${PYTHON_IMAGE} AS build
ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    UV_PROJECT_ENVIRONMENT=/app/.venv
# Behind a TLS-intercepting proxy, pass its CA as a build secret (never stored in the image):
#   docker build --secret id=build_ca,src=/path/to/ca.crt --build-arg HTTPS_PROXY=... .
RUN --mount=type=secret,id=build_ca,required=false \
    if [ -f /run/secrets/build_ca ]; then export PIP_CERT=/run/secrets/build_ca; fi; \
    pip install "uv==0.11.32"
WORKDIR /app
# Dependencies first (cached until uv.lock changes), then the project itself.
COPY pyproject.toml uv.lock README.md ./
RUN --mount=type=secret,id=build_ca,required=false \
    if [ -f /run/secrets/build_ca ]; then export SSL_CERT_FILE=/run/secrets/build_ca; fi; \
    uv sync --locked --no-dev --no-install-project
COPY src ./src
RUN --mount=type=secret,id=build_ca,required=false \
    if [ -f /run/secrets/build_ca ]; then export SSL_CERT_FILE=/run/secrets/build_ca; fi; \
    uv sync --locked --no-dev --no-editable

# ---------------------------------------------------------------- runtime: no build tools, non-root
FROM ${PYTHON_IMAGE} AS runtime
RUN groupadd --system --gid 10001 trader \
 && useradd --system --uid 10001 --gid trader --home-dir /app --shell /usr/sbin/nologin trader \
 && mkdir -p /app/data && chown trader:trader /app/data
WORKDIR /app
COPY --from=build /app/.venv /app/.venv
COPY config ./config
ENV PATH="/app/.venv/bin:${PATH}" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    TRADER_OPS_PORT=9108
USER trader
# Books, OMS snapshots, the WAL, recorded ticks and reports. Mount a volume: it is the process's memory.
VOLUME ["/app/data"]
EXPOSE 9108 8700
# Liveness only: a restart is safe (startup always reconciles with the broker first). Readiness (/health)
# also needs every broker connected; it is for monitoring, not for restarts.
HEALTHCHECK --interval=15s --timeout=5s --start-period=180s --retries=3 \
  CMD ["python", "-c", "import os, urllib.request; urllib.request.urlopen(f\"http://127.0.0.1:{os.environ['TRADER_OPS_PORT']}/health/live\", timeout=4)"]
# trader run handles SIGTERM: pause strategies, let in-flight orders settle, persist, disconnect.
STOPSIGNAL SIGTERM
ENTRYPOINT ["trader"]
CMD ["run", "--config", "config/docker.yaml"]
