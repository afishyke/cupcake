#!/bin/bash
#
# Docker health check script for Tick-Tick Stock Monitoring Program
#

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Configuration
REDIS_HOST="${REDIS_HOST:-redis}"
REDIS_PORT="${REDIS_PORT:-6379}"
REDIS_DB="${REDIS_DB:-0}"
LOG_DIR="${CUPCAKE_LOG_DIR:-/app/logs}"

# Function to check Redis connectivity
check_redis() {
    if redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" -n "$REDIS_DB" ping >/dev/null 2>&1; then
        echo -e "${GREEN}✅ Redis connection: OK${NC}"
        return 0
    else
        echo -e "${RED}❌ Redis connection: FAILED${NC}"
        return 1
    fi
}

# Function to check if logs directory is writable
check_logs() {
    if [ -d "$LOG_DIR" ] && [ -w "$LOG_DIR" ]; then
        echo -e "${GREEN}✅ Logs directory: OK${NC}"
        return 0
    else
        echo -e "${RED}❌ Logs directory: NOT WRITABLE${NC}"
        return 1
    fi
}

# Function to check if credentials file exists
check_credentials() {
    if [ -f "/app/authentication/credentials.json" ]; then
        echo -e "${GREEN}✅ Credentials file: OK${NC}"
        return 0
    else
        echo -e "${YELLOW}⚠️ Credentials file: MISSING (may be normal for some services)${NC}"
        return 0  # Don't fail health check for missing credentials
    fi
}

# Function to check if symbols file exists
check_symbols() {
    if [ -f "/app/scripts/symbols.json" ]; then
        echo -e "${GREEN}✅ Symbols file: OK${NC}"
        return 0
    else
        echo -e "${RED}❌ Symbols file: MISSING${NC}"
        return 1
    fi
}

# Function to check if protobuf files are compiled
check_protobuf() {
    if [ -f "/app/scripts/MarketDataFeed_pb2.py" ]; then
        echo -e "${GREEN}✅ Protobuf compiled: OK${NC}"
        return 0
    else
        echo -e "${RED}❌ Protobuf compiled: MISSING${NC}"
        return 1
    fi
}

# Function to check Python import capabilities
check_python_imports() {
    local required_modules=(
        "redis"
        "numpy"
        "pandas" 
        "requests"
        "json"
        "datetime"
        "pytz"
    )
    
    local failed_imports=0
    
    for module in "${required_modules[@]}"; do
        if python3 -c "import $module" >/dev/null 2>&1; then
            echo -e "${GREEN}✅ Python module $module: OK${NC}"
        else
            echo -e "${RED}❌ Python module $module: FAILED${NC}"
            ((failed_imports++))
        fi
    done
    
    if [ $failed_imports -eq 0 ]; then
        return 0
    else
        return 1
    fi
}

# Function to check disk space for logs
check_disk_space() {
    local log_disk_usage
    log_disk_usage=$(df "$LOG_DIR" | awk 'NR==2 {print $5}' | sed 's/%//')
    
    if [ "$log_disk_usage" -lt 90 ]; then
        echo -e "${GREEN}✅ Disk space: ${log_disk_usage}% used${NC}"
        return 0
    else
        echo -e "${YELLOW}⚠️ Disk space: ${log_disk_usage}% used (WARNING)${NC}"
        return 0  # Don't fail health check, just warn
    fi
}

# Function to check if TimeSeries data exists in Redis
check_timeseries_data() {
    local ts_keys_count
    ts_keys_count=$(redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" -n "$REDIS_DB" KEYS "stock:*" 2>/dev/null | wc -l)
    
    if [ "$ts_keys_count" -gt 0 ]; then
        echo -e "${GREEN}✅ TimeSeries data: $ts_keys_count keys found${NC}"
        return 0
    else
        echo -e "${YELLOW}⚠️ TimeSeries data: No data found (may be normal during startup)${NC}"
        return 0  # Don't fail during startup
    fi
}

# Main health check function
main() {
    echo "🔍 Docker Health Check - $(date)"
    echo "================================"
    
    local checks_passed=0
    local total_checks=0
    
    # Essential checks (must pass)
    essential_checks=(
        "check_redis"
        "check_logs" 
        "check_symbols"
        "check_protobuf"
        "check_python_imports"
    )
    
    # Optional checks (warnings only)
    optional_checks=(
        "check_credentials"
        "check_disk_space"
        "check_timeseries_data"
    )
    
    echo "Essential Checks:"
    echo "-----------------"
    for check in "${essential_checks[@]}"; do
        ((total_checks++))
        if $check; then
            ((checks_passed++))
        fi
    done
    
    echo ""
    echo "Optional Checks:"
    echo "----------------"
    for check in "${optional_checks[@]}"; do
        $check  # These don't affect the health check result
    done
    
    echo ""
    echo "Health Check Summary:"
    echo "====================="
    echo "Essential checks passed: $checks_passed/$total_checks"
    
    if [ $checks_passed -eq $total_checks ]; then
        echo -e "${GREEN}✅ Overall Status: HEALTHY${NC}"
        exit 0
    else
        echo -e "${RED}❌ Overall Status: UNHEALTHY${NC}"
        exit 1
    fi
}

# Run main function
main "$@"