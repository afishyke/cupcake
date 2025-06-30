#!/bin/bash

# Docker Container Internal Runner
# This script is designed to run inside the Docker container

echo "🐳 Docker Container Trading System Runner"
echo "=========================================="

# Set default mode
MODE=${1:-full}

# Show available modes
echo "🎯 Running in mode: $MODE"
echo ""
echo "Available modes:"
echo "  full        - Complete system with live data, signals, and web dashboard"
echo "  live        - Live data feed and signals only"
echo "  historical  - Historical data viewer only"  
echo "  backfill    - Run historical data backfill only"
echo "  viewer      - Interactive historical data viewer CLI"
echo ""

# Check if we're inside a Docker container
if [ ! -f /.dockerenv ]; then
    echo "⚠️  This script is designed to run inside a Docker container"
    echo "   Use 'docker-compose up' or './start.sh' from the host instead"
    exit 1
fi

# Set up environment
export PYTHONPATH=/app:$PYTHONPATH
cd /app

# Check required files
if [ ! -f "/app/credentials.json" ]; then
    echo "❌ credentials.json not found at /app/credentials.json"
    echo "   Please mount your credentials.json file to the container"
    exit 1
fi

if [ ! -f "/app/symbols.json" ]; then
    echo "❌ symbols.json not found at /app/symbols.json"
    echo "   Please mount your symbols.json file to the container"
    exit 1
fi

# Create directories if they don't exist
mkdir -p /app/logs /app/config

# Show container info
echo "📊 Container Environment:"
echo "   • Working Directory: $(pwd)"
echo "   • Python Version: $(python3 --version)"
echo "   • App Host: ${APP_HOST:-0.0.0.0}"
echo "   • App Port: ${APP_PORT:-5000}"
echo "   • Redis Host: ${REDIS_HOST:-redis}"
echo ""

# Run the Python application
echo "🚀 Starting Enhanced Trading System..."
echo "=========================================="

exec python3 main.py --mode "$MODE" --port "${APP_PORT:-5000}"