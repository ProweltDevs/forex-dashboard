#!/usr/bin/env bash
# Smoke test of the container stack (CI and local): build the image, start PostgreSQL + the app (paper
# mode, config/docker.yaml), wait until /health is ready, stop the app with SIGTERM and require a graceful
# exit (code 0), then remove everything, volumes included.
#   scripts/docker_smoke.sh                  # build with docker compose
#   SKIP_BUILD=1 TRADER_IMAGE=trader:local scripts/docker_smoke.sh   # use an image you built yourself
set -euo pipefail

project="${COMPOSE_PROJECT:-trader-smoke}"
compose=(docker compose -p "$project")
cleanup() { "${compose[@]}" down -v --remove-orphans >/dev/null 2>&1 || true; }
trap cleanup EXIT

if [[ -z "${SKIP_BUILD:-}" ]]; then
  "${compose[@]}" build app
fi
"${compose[@]}" up -d --no-build

echo "waiting for /health ..."
for _ in $(seq 1 90); do
  code="$(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:9108/health || true)"
  [[ "$code" == "200" ]] && break
  sleep 2
done
curl -sf http://127.0.0.1:9108/health
echo
[[ "$code" == "200" ]] || { "${compose[@]}" logs app | tail -50; echo "not ready"; exit 1; }
curl -sf http://127.0.0.1:9108/metrics | grep -E '^trader_(broker_connected|db_up|account_ready)'

container="$("${compose[@]}" ps -q app)"
for _ in $(seq 1 30); do  # the image's HEALTHCHECK probes /health/live every 15 s
  health="$(docker inspect -f '{{.State.Health.Status}}' "$container")"
  [[ "$health" == "healthy" ]] && break
  sleep 2
done
echo "docker health: $health"
[[ "$health" == "healthy" ]]

"${compose[@]}" stop app
exit_code="$(docker inspect -f '{{.State.ExitCode}}' "$container")"
"${compose[@]}" logs app | grep -q '"event": "app_stopped"' || { echo "no graceful shutdown"; exit 1; }
echo "graceful stop, exit code $exit_code"
[[ "$exit_code" == "0" ]]
