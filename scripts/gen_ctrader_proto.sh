#!/usr/bin/env bash
# Regenerate the cTrader Open API Python bindings from the vendored .proto files.
# The .proto files are pinned (see src/trader/brokers/ctrader/proto/src/VERSION); bump them deliberately.
set -euo pipefail
cd "$(dirname "$0")/.."
PROTO=src/trader/brokers/ctrader/proto
rm -f "$PROTO"/*_pb2.py "$PROTO"/*_pb2.pyi
uv run python -m grpc_tools.protoc -I "$PROTO/src" \
  --python_out="$PROTO" --mypy_out="$PROTO" \
  "$PROTO"/src/*.proto
# protoc emits absolute imports ("import OpenApiCommonMessages_pb2"); make them package-relative.
for f in "$PROTO"/*_pb2.py "$PROTO"/*_pb2.pyi; do
  sed -i -E 's/^import (OpenApi[A-Za-z]+_pb2)/from . import \1/' "$f"
done
