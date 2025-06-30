#!/bin/bash

# Enhanced Algorithmic Trading System Runner (Docker Version)
# This script runs the complete trading system with all features in Docker

echo "🚀 Starting Enhanced Algorithmic Trading System (Docker)..."
echo "=================================================="

# Change to the script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "📁 Working directory: $SCRIPT_DIR"

# Check if running inside Docker container
if [ -f /.dockerenv ]; then
    echo "🐳 Running inside Docker container"
    # If inside container, just run the Python application directly
    MODE=${1:-full}
    echo "🎯 Starting in mode: $MODE"
    python3 main.py --mode "$MODE"
    exit $?
fi

# Running on host - use Docker Compose
echo "🐳 Running on host - using Docker Compose"

# Check if Docker and Docker Compose are available
if ! command -v docker &> /dev/null; then
    echo "❌ Docker not found! Please install Docker"
    exit 1
fi

if ! command -v docker-compose &> /dev/null; then
    echo "❌ Docker Compose not found! Please install Docker Compose"
    exit 1
fi

# Check if required files exist
if [ ! -f "credentials.json" ]; then
    echo "❌ credentials.json not found!"
    echo "Please create credentials.json with your Upstox API credentials"
    exit 1
fi

if [ ! -f "symbols.json" ]; then
    echo "❌ symbols.json not found!"
    echo "Please ensure symbols.json exists in the project directory"
    exit 1
fi

# Check for .env file
if [ ! -f ".env" ]; then
    echo "⚠️  .env file not found, creating from template..."
    if [ -f ".env.example" ]; then
        cp .env.example .env
        echo "✅ Created .env from template"
    else
        echo "Creating basic .env file..."
        cat > .env << EOF
APP_HOST=0.0.0.0
APP_PORT=5000
REDIS_HOST=redis
REDIS_PORT=6379
EOF
        echo "✅ Created basic .env file"
    fi
fi

# Create required directories
mkdir -p logs config
chmod 755 logs config

echo ""
echo "📊 System Features:"
echo "   • Ultra-Fast Historical Data Fetching"
echo "   • Real-time Market Data Streaming" 
echo "   • Actionable Trading Signals"
echo "   • Trajectory Confirmation Analysis"
echo "   • Multi-timeframe Technical Analysis"
echo "   • Real-time Portfolio Tracking"
echo "   • Web Dashboard Interface"
echo "   • Paper Trading Engine"
echo ""

# Check if containers are already running
if docker-compose ps | grep -q "Up"; then
    echo "🔄 Containers already running. Restarting..."
    docker-compose down
fi

# Build and start containers
echo "🔨 Building and starting Docker containers..."
echo "=================================================="

# Start with Docker Compose
docker-compose up --build

# Handle exit
EXIT_CODE=$?
echo ""
echo "👋 Trading System shut down with exit code: $EXIT_CODE"

if [ $EXIT_CODE -eq 0 ]; then
    echo "✅ System shut down successfully"
else
    echo "❌ System encountered an error during shutdown"
fi