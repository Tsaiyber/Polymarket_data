#!/bin/bash
# Clean and process raw on-chain data

set -e

echo "Processing on-chain data..."
uv run polymarket process
echo "✓ Data processing completed"
