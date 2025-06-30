#!/bin/bash

# Quick Start Script for Enhanced Trading System (Docker Version)
# Double-click this file to run the complete trading system

echo "🚀 Quick Starting Enhanced Trading System (Docker)..."

# Change to script directory
cd "$(dirname "${BASH_SOURCE[0]}")"

# Check if running inside Docker container
if [ -f /.dockerenv ]; then
    echo "🐳 Running inside Docker container"
    # Run the full system directly
    python3 main.py --mode full
else
    echo "🐳 Running on host - using Docker Compose"
    # Use Docker Compose to start the system
    docker-compose up --build
fi

echo "👋 Trading System stopped."