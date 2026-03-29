#!/bin/bash
# Fetch market metadata from Gamma API

set -e

echo "Fetching market data from Gamma API..."
uv run polymarket fetch-markets
echo "✓ Market data fetched successfully"
