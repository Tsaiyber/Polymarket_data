#!/bin/bash
# Full update: fetch markets, refresh market states, fetch on-chain data, and process

set -e

echo "=== Full Update Pipeline ==="
echo ""

echo "[1/1] Running full update pipeline..."
uv run polymarket update
echo ""

echo "✓ Full update completed"
