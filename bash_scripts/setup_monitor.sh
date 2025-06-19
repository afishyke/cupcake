#!/bin/bash

# Setup script for Redis Rolling Window Monitor
# This script sets up the monitoring environment and provides quick access

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MONITOR_SCRIPT="$SCRIPT_DIR/monitor_rolling_window.sh"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${BLUE}🚀 Redis Rolling Window Monitor Setup${NC}"
echo -e "${BLUE}═══════════════════════════════════════${NC}"

# Check if monitor script exists
if [[ ! -f "$MONITOR_SCRIPT" ]]; then
    echo -e "${RED}❌ Monitor script not found: $MONITOR_SCRIPT${NC}"
    exit 1
fi

# Make monitor script executable
chmod +x "$MONITOR_SCRIPT"
echo -e "${GREEN}✅ Monitor script made executable${NC}"

# Check dependencies
echo -e "\n${YELLOW}📦 Checking dependencies...${NC}"

# Check Redis CLI
if command -v redis-cli >/dev/null 2>&1; then
    echo -e "${GREEN}✅ redis-cli found${NC}"
else
    echo -e "${RED}❌ redis-cli not found${NC}"
    echo -e "${YELLOW}📥 Installing redis-tools...${NC}"
    if command -v apt-get >/dev/null 2>&1; then
        sudo apt-get update && sudo apt-get install -y redis-tools
    elif command -v yum >/dev/null 2>&1; then
        sudo yum install -y redis
    elif command -v brew >/dev/null 2>&1; then
        brew install redis
    else
        echo -e "${RED}❌ Cannot install redis-cli automatically. Please install manually.${NC}"
        exit 1
    fi
fi

# Check bc calculator
if command -v bc >/dev/null 2>&1; then
    echo -e "${GREEN}✅ bc calculator found${NC}"
else
    echo -e "${RED}❌ bc calculator not found${NC}"
    echo -e "${YELLOW}📥 Installing bc...${NC}"
    if command -v apt-get >/dev/null 2>&1; then
        sudo apt-get install -y bc
    elif command -v yum >/dev/null 2>&1; then
        sudo yum install -y bc
    elif command -v brew >/dev/null 2>&1; then
        brew install bc
    else
        echo -e "${RED}❌ Cannot install bc automatically. Please install manually.${NC}"
        exit 1
    fi
fi

# Test Redis connection
echo -e "\n${YELLOW}🔗 Testing Redis connection...${NC}"
REDIS_HOST="${REDIS_HOST:-localhost}"
REDIS_PORT="${REDIS_PORT:-6379}"
REDIS_DB="${REDIS_DB:-1}"

if redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" -n "$REDIS_DB" ping >/dev/null 2>&1; then
    echo -e "${GREEN}✅ Redis connection successful${NC}"
    
    # Check for existing data
    KEYS_COUNT=$(redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" -n "$REDIS_DB" KEYS "ts:*" 2>/dev/null | wc -l)
    if [ "$KEYS_COUNT" -gt 0 ]; then
        echo -e "${GREEN}✅ Found $KEYS_COUNT TimeSeries keys in Redis${NC}"
    else
        echo -e "${YELLOW}⚠️ No TimeSeries data found in Redis${NC}"
        echo -e "${YELLOW}💡 Make sure your analytics engine is running and has processed some market data${NC}"
    fi
else
    echo -e "${RED}❌ Cannot connect to Redis at $REDIS_HOST:$REDIS_PORT${NC}"
    echo -e "${YELLOW}💡 Make sure Redis is running: sudo systemctl start redis${NC}"
fi

# Create convenient aliases
echo -e "\n${YELLOW}🔧 Creating convenient aliases...${NC}"

BASHRC_FILE="$HOME/.bashrc"
ALIAS_SECTION="# Redis Market Monitor Aliases"

# Remove existing aliases if present
if grep -q "$ALIAS_SECTION" "$BASHRC_FILE" 2>/dev/null; then
    sed -i "/$ALIAS_SECTION/,/# End Redis Market Monitor Aliases/d" "$BASHRC_FILE"
fi

# Add new aliases
cat >> "$BASHRC_FILE" << EOF

$ALIAS_SECTION
alias market-overview='$MONITOR_SCRIPT overview'
alias market-live='$MONITOR_SCRIPT overview auto'
alias market-tcs='$MONITOR_SCRIPT dashboard TATA_CONSULTANCY_SERVICES'
alias market-reliance='$MONITOR_SCRIPT dashboard RELIANCE_INDUSTRIES'
alias market-keys='$MONITOR_SCRIPT keys'
alias market-help='$MONITOR_SCRIPT help'
# End Redis Market Monitor Aliases
EOF

echo -e "${GREEN}✅ Aliases added to ~/.bashrc${NC}"

# Create desktop shortcuts (optional)
if command -v gnome-terminal >/dev/null 2>&1; then
    DESKTOP_DIR="$HOME/Desktop"
    if [[ -d "$DESKTOP_DIR" ]]; then
        cat > "$DESKTOP_DIR/Market_Monitor.desktop" << EOF
[Desktop Entry]
Version=1.0
Type=Application
Name=Market Monitor
Comment=Real-time market data monitor
Exec=gnome-terminal -- $MONITOR_SCRIPT overview auto
Icon=utilities-system-monitor
Terminal=true
Categories=Development;
EOF
        chmod +x "$DESKTOP_DIR/Market_Monitor.desktop"
        echo -e "${GREEN}✅ Desktop shortcut created${NC}"
    fi
fi

# Display usage instructions
echo -e "\n${BLUE}🎉 Setup Complete!${NC}"
echo -e "${BLUE}═════════════════${NC}"

echo -e "\n${YELLOW}📋 Quick Start Commands:${NC}"
echo -e "  ${GREEN}$MONITOR_SCRIPT overview${NC}          # Market overview"
echo -e "  ${GREEN}$MONITOR_SCRIPT overview auto${NC}      # Auto-refreshing overview"  
echo -e "  ${GREEN}$MONITOR_SCRIPT dashboard TCS${NC}      # TCS detailed dashboard"
echo -e "  ${GREEN}$MONITOR_SCRIPT keys${NC}               # Show all available data"

echo -e "\n${YELLOW}🔗 New Aliases (restart terminal or run 'source ~/.bashrc'):${NC}"
echo -e "  ${GREEN}market-overview${NC}                    # Market overview"
echo -e "  ${GREEN}market-live${NC}                        # Live auto-refreshing view"
echo -e "  ${GREEN}market-tcs${NC}                         # TCS detailed dashboard"
echo -e "  ${GREEN}market-reliance${NC}                    # Reliance detailed dashboard"
echo -e "  ${GREEN}market-keys${NC}                        # Show available data keys"
echo -e "  ${GREEN}market-help${NC}                        # Show help"

echo -e "\n${YELLOW}⚙️  Environment Variables:${NC}"
echo -e "  ${GREEN}export REDIS_HOST=localhost${NC}       # Redis hostname"
echo -e "  ${GREEN}export REDIS_PORT=6379${NC}            # Redis port"
echo -e "  ${GREEN}export REDIS_DB=1${NC}                 # Redis database"
echo -e "  ${GREEN}export REFRESH_INTERVAL=2${NC}         # Refresh interval (seconds)"

echo -e "\n${YELLOW}🚀 To start monitoring right now:${NC}"
echo -e "  ${GREEN}$MONITOR_SCRIPT overview auto${NC}"

echo -e "\n${BLUE}💡 Pro Tips:${NC}"
echo -e "  • Use ${GREEN}Ctrl+C${NC} to stop any running monitor"
echo -e "  • Run ${GREEN}market-live${NC} in a separate terminal for continuous monitoring"
echo -e "  • Check ${GREEN}market-keys${NC} to see all available metrics"
echo -e "  • Use ${GREEN}market-help${NC} to see all available commands"

# Source bashrc to make aliases available immediately
echo -e "\n${YELLOW}🔄 Reloading bash configuration...${NC}"
if [[ -f "$BASHRC_FILE" ]]; then
    source "$BASHRC_FILE" 2>/dev/null || true
fi

echo -e "\n${GREEN}✅ Redis Rolling Window Monitor is ready to use!${NC}"
