#!/bin/bash
# Start continuous blockchain data fetching

set -e

echo "Starting continuous data fetching..."

# Create directories
mkdir -p logs data

# Start the process in background
nohup uv run python -m polymarket.tools.continuous_fetch > logs/continuous_fetch.log 2>&1 &

PID=$!
echo $PID > logs/continuous_fetch.pid

echo "✓ Continuous fetching started"
echo "  Process ID: $PID"
echo "  Log file: logs/continuous_fetch.log"
echo ""
echo "To view logs: tail -f logs/continuous_fetch.log"
echo "To stop: ./scripts/continuous_stop.sh"
