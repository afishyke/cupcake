#!/bin/bash

# Redis Rolling Window Monitor
# Monitor real-time market analytics data from Redis TimeSeries
# Usage: ./monitor_rolling_window.sh [options]

set -euo pipefail

# Configuration
REDIS_HOST="${REDIS_HOST:-localhost}"
REDIS_PORT="${REDIS_PORT:-6379}"
REDIS_DB="${REDIS_DB:-1}"
REFRESH_INTERVAL="${REFRESH_INTERVAL:-2}"
LOG_DIR="${CUPCAKE_LOG_DIR:-/home/abhishek/projects/CUPCAKE/logs}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
WHITE='\033[1;37m'
NC='\033[0m' # No Color

# Symbols mapping (should match your symbols.json)
declare -A SYMBOLS=(
    ["RELIANCE_INDUSTRIES"]="RELIANCE"
    ["TATA_CONSULTANCY_SERVICES"]="TCS"
    ["INFOSYS"]="INFY"
    ["HDFC_BANK"]="HDFCBANK"
    ["ICICI_BANK"]="ICICIBANK"
    ["HINDUSTAN_UNILEVER"]="HUL"
    ["ITC"]="ITC"
    ["STATE_BANK_OF_INDIA"]="SBIN"
    ["BHARTI_AIRTEL"]="BHARTIARTL"
    ["KOTAK_MAHINDRA_BANK"]="KOTAKBANK"
    ["LARSEN_&_TOUBRO"]="LT"
    ["ASIAN_PAINTS"]="ASIANPAINT"
    ["MARUTI_SUZUKI"]="MARUTI"
    ["BAJAJ_FINANCE"]="BAJFINANCE"
    ["HCL_TECHNOLOGIES"]="HCLTECH"
)

# Available metrics
METRICS=(
    "ltp" "mid_price" "spread" "spread_bps" "volume"
    "vwap_1m" "vwap_5m" "vwap_15m" "vwma_10" "vwma_30"
    "true_range" "atr_14" "parkinson_vol" "roc_1m" "roc_5m"
    "ema_9" "ema_21" "imbalance" "liquidity_score"
)

# Function to check Redis connection
check_redis_connection() {
    if ! redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" -n "$REDIS_DB" ping >/dev/null 2>&1; then
        echo -e "${RED}❌ Cannot connect to Redis at $REDIS_HOST:$REDIS_PORT (DB: $REDIS_DB)${NC}"
        echo -e "${YELLOW}💡 Make sure Redis is running and the analytics engine has written data${NC}"
        exit 1
    fi
    echo -e "${GREEN}✅ Connected to Redis at $REDIS_HOST:$REDIS_PORT (DB: $REDIS_DB)${NC}"
}

# Function to get latest value from TimeSeries
get_latest_value() {
    local key="$1"
    local result
    result=$(redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" -n "$REDIS_DB" \
        TS.GET "$key" 2>/dev/null | tail -1)
    echo "$result"
}

# Function to get range of values from TimeSeries
get_range_values() {
    local key="$1"
    local count="${2:-10}"
    local end_time=$(($(date +%s) * 1000))
    local start_time=$((end_time - (count * 60 * 1000)))  # last N minutes
    
    redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" -n "$REDIS_DB" \
        TS.RANGE "$key" "$start_time" "$end_time" AGGREGATION avg 60000 2>/dev/null || echo ""
}

# Function to get recent N data points
get_recent_points() {
    local key="$1"
    local count="${2:-20}"
    local end_time=$(($(date +%s) * 1000))
    local start_time=$((end_time - (count * 60 * 1000)))
    
    redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" -n "$REDIS_DB" \
        TS.RANGE "$key" "$start_time" "$end_time" 2>/dev/null || echo ""
}

# Function to create ASCII chart
create_ascii_chart() {
    local -a values=("$@")
    local max_val min_val range
    local chart_height=8
    local chart_width=${#values[@]}
    
    if [ ${#values[@]} -eq 0 ]; then
        echo "No data available"
        return
    fi
    
    # Find min and max
    max_val=${values[0]}
    min_val=${values[0]}
    for val in "${values[@]}"; do
        if (( $(echo "$val > $max_val" | bc -l) )); then
            max_val=$val
        fi
        if (( $(echo "$val < $min_val" | bc -l) )); then
            min_val=$val
        fi
    done
    
    range=$(echo "$max_val - $min_val" | bc -l)
    if (( $(echo "$range == 0" | bc -l) )); then
        range=1
    fi
    
    # Create chart
    echo -e "${CYAN}┌$(printf '─%.0s' $(seq 1 $((chart_width + 2))))┐${NC}"
    for ((row = chart_height; row >= 0; row--)); do
        local threshold=$(echo "$min_val + ($range * $row / $chart_height)" | bc -l)
        printf "${CYAN}│${NC}"
        for val in "${values[@]}"; do
            if (( $(echo "$val >= $threshold" | bc -l) )); then
                printf "${GREEN}█${NC}"
            else
                printf " "
            fi
        done
        printf "${CYAN}│${NC} "
        if [ $row -eq $chart_height ]; then
            printf "%.3f" "$max_val"
        elif [ $row -eq 0 ]; then
            printf "%.3f" "$min_val"
        fi
        echo
    done
    echo -e "${CYAN}└$(printf '─%.0s' $(seq 1 $((chart_width + 2))))┘${NC}"
}

# Function to display dashboard for single instrument
show_instrument_dashboard() {
    local instrument="$1"
    local short_name="${SYMBOLS[$instrument]:-$instrument}"
    
    echo -e "\n${WHITE}═══════════════════════════════════════════════════════════════════════════════${NC}"
    echo -e "${WHITE}🎯 $short_name ($instrument) - $(date '+%H:%M:%S')${NC}"
    echo -e "${WHITE}═══════════════════════════════════════════════════════════════════════════════${NC}"
    
    # Price metrics
    local ltp=$(get_latest_value "ts:ltp:$instrument")
    local mid_price=$(get_latest_value "ts:mid_price:$instrument")
    local spread=$(get_latest_value "ts:spread:$instrument")
    local spread_bps=$(get_latest_value "ts:spread_bps:$instrument")
    
    echo -e "${CYAN}💰 PRICE METRICS${NC}"
    printf "   %-15s: %s\n" "LTP" "₹${ltp:-N/A}"
    printf "   %-15s: %s\n" "Mid Price" "₹${mid_price:-N/A}"
    printf "   %-15s: %s (%.1f bps)\n" "Spread" "₹${spread:-N/A}" "${spread_bps:-0}"
    
    # VWAP metrics
    local vwap_1m=$(get_latest_value "ts:vwap_1m:$instrument")
    local vwap_5m=$(get_latest_value "ts:vwap_5m:$instrument")
    local vwap_15m=$(get_latest_value "ts:vwap_15m:$instrument")
    
    echo -e "\n${PURPLE}📈 VWAP ANALYSIS${NC}"
    printf "   %-15s: %s\n" "VWAP 1m" "₹${vwap_1m:-N/A}"
    printf "   %-15s: %s\n" "VWAP 5m" "₹${vwap_5m:-N/A}"
    printf "   %-15s: %s\n" "VWAP 15m" "₹${vwap_15m:-N/A}"
    
    # VWAP alignment signal
    if [[ -n "$ltp" && -n "$vwap_1m" && -n "$vwap_5m" && -n "$vwap_15m" ]]; then
        if (( $(echo "$ltp > $vwap_1m && $ltp > $vwap_5m && $ltp > $vwap_15m" | bc -l) )); then
            echo -e "   ${GREEN}🚀 Signal: ALL VWAP BULLISH${NC}"
        elif (( $(echo "$ltp < $vwap_1m && $ltp < $vwap_5m && $ltp < $vwap_15m" | bc -l) )); then
            echo -e "   ${RED}🔻 Signal: ALL VWAP BEARISH${NC}"
        else
            echo -e "   ${YELLOW}😐 Signal: VWAP MIXED${NC}"
        fi
    fi
    
    # Technical indicators
    local ema_9=$(get_latest_value "ts:ema_9:$instrument")
    local ema_21=$(get_latest_value "ts:ema_21:$instrument")
    local atr_14=$(get_latest_value "ts:atr_14:$instrument")
    local parkinson_vol=$(get_latest_value "ts:parkinson_vol:$instrument")
    
    echo -e "\n${BLUE}📊 TECHNICAL INDICATORS${NC}"
    printf "   %-15s: %s\n" "EMA 9" "₹${ema_9:-N/A}"
    printf "   %-15s: %s\n" "EMA 21" "₹${ema_21:-N/A}"
    printf "   %-15s: %s\n" "ATR 14" "₹${atr_14:-N/A}"
    printf "   %-15s: %.4f\n" "Parkinson Vol" "${parkinson_vol:-0}"
    
    # Momentum
    local roc_1m=$(get_latest_value "ts:roc_1m:$instrument")
    local roc_5m=$(get_latest_value "ts:roc_5m:$instrument")
    
    echo -e "\n${YELLOW}🚀 MOMENTUM${NC}"
    printf "   %-15s: %.4f%%\n" "ROC 1m" "$(echo "${roc_1m:-0} * 100" | bc -l)"
    printf "   %-15s: %.4f%%\n" "ROC 5m" "$(echo "${roc_5m:-0} * 100" | bc -l)"
    
    # Order book
    local imbalance=$(get_latest_value "ts:imbalance:$instrument")
    local liquidity=$(get_latest_value "ts:liquidity_score:$instrument")
    
    echo -e "\n${GREEN}⚖️  ORDER BOOK${NC}"
    printf "   %-15s: %.3f\n" "Imbalance" "${imbalance:-0}"
    printf "   %-15s: %.1f/100\n" "Liquidity Score" "${liquidity:-0}"
    
    # Recent price trend (ASCII chart)
    echo -e "\n${WHITE}📈 RECENT PRICE TREND (Last 20 points)${NC}"
    local price_data=$(get_recent_points "ts:ltp:$instrument" 20)
    if [[ -n "$price_data" ]]; then
        local -a prices=()
        while IFS= read -r line; do
            if [[ $line =~ [0-9]+.*[0-9]+ ]]; then
                local price=$(echo "$line" | awk '{print $2}')
                prices+=("$price")
            fi
        done <<< "$price_data"
        
        if [ ${#prices[@]} -gt 0 ]; then
            create_ascii_chart "${prices[@]}"
        else
            echo "   No recent price data available"
        fi
    else
        echo "   No price data available"
    fi
}

# Function to show compact overview of all instruments
show_overview() {
    echo -e "\n${WHITE}📊 MARKET OVERVIEW - $(date '+%Y-%m-%d %H:%M:%S')${NC}"
    echo -e "${WHITE}════════════════════════════════════════════════════════════════════════════════════════${NC}"
    printf "${WHITE}%-12s %10s %10s %10s %10s %8s %8s %s${NC}\n" \
           "SYMBOL" "LTP" "VWAP_1m" "VWAP_5m" "ATR" "ROC_1m" "LIQ" "SIGNALS"
    echo -e "${WHITE}────────────────────────────────────────────────────────────────────────────────────────${NC}"
    
    for instrument in "${!SYMBOLS[@]}"; do
        local short_name="${SYMBOLS[$instrument]}"
        local ltp=$(get_latest_value "ts:ltp:$instrument")
        local vwap_1m=$(get_latest_value "ts:vwap_1m:$instrument")
        local vwap_5m=$(get_latest_value "ts:vwap_5m:$instrument")
        local vwap_15m=$(get_latest_value "ts:vwap_15m:$instrument")
        local atr=$(get_latest_value "ts:atr_14:$instrument")
        local roc_1m=$(get_latest_value "ts:roc_1m:$instrument")
        local liquidity=$(get_latest_value "ts:liquidity_score:$instrument")
        
        # Generate signals
        local signals=""
        if [[ -n "$ltp" && -n "$vwap_1m" && -n "$vwap_5m" && -n "$vwap_15m" ]]; then
            if (( $(echo "$ltp > $vwap_1m && $ltp > $vwap_5m && $ltp > $vwap_15m" | bc -l) )); then
                signals="${GREEN}🚀BULL${NC}"
            elif (( $(echo "$ltp < $vwap_1m && $ltp < $vwap_5m && $ltp < $vwap_15m" | bc -l) )); then
                signals="${RED}🔻BEAR${NC}"
            else
                signals="${YELLOW}😐MIX${NC}"
            fi
        fi
        
        # Color code ROC
        local roc_color="${NC}"
        if [[ -n "$roc_1m" ]]; then
            if (( $(echo "$roc_1m > 0.001" | bc -l) )); then
                roc_color="${GREEN}"
            elif (( $(echo "$roc_1m < -0.001" | bc -l) )); then
                roc_color="${RED}"
            fi
        fi
        
        printf "%-12s %10.2f %10.2f %10.2f %10.2f ${roc_color}%7.3f%%${NC} %7.0f %s\n" \
               "$short_name" \
               "${ltp:-0}" \
               "${vwap_1m:-0}" \
               "${vwap_5m:-0}" \
               "${atr:-0}" \
               "$(echo "${roc_1m:-0} * 100" | bc -l)" \
               "${liquidity:-0}" \
               "$signals"
    done
}

# Function to show available TimeSeries keys
show_available_keys() {
    echo -e "\n${WHITE}📋 AVAILABLE TIMESERIES KEYS${NC}"
    echo -e "${WHITE}═══════════════════════════════════════════════════════════════${NC}"
    
    local keys=$(redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" -n "$REDIS_DB" KEYS "ts:*" 2>/dev/null | sort)
    
    if [[ -z "$keys" ]]; then
        echo -e "${RED}❌ No TimeSeries keys found${NC}"
        echo -e "${YELLOW}💡 Make sure the analytics engine is running and has processed data${NC}"
        return
    fi
    
    echo "$keys" | head -20
    local total_keys=$(echo "$keys" | wc -l)
    if [ "$total_keys" -gt 20 ]; then
        echo -e "${YELLOW}... and $((total_keys - 20)) more keys${NC}"
    fi
    echo -e "${GREEN}Total: $total_keys TimeSeries keys${NC}"
}

# Function to show real-time streaming data
stream_data() {
    local instrument="${1:-TATA_CONSULTANCY_SERVICES}"
    local metric="${2:-ltp}"
    
    echo -e "${WHITE}📡 STREAMING $metric for $instrument (Press Ctrl+C to stop)${NC}"
    echo -e "${WHITE}═══════════════════════════════════════════════════════════════${NC}"
    
    while true; do
        local value=$(get_latest_value "ts:$metric:$instrument")
        local timestamp=$(date '+%H:%M:%S')
        printf "${GREEN}[%s]${NC} %s: %s\n" "$timestamp" "$metric" "${value:-N/A}"
        sleep "$REFRESH_INTERVAL"
    done
}

# Function to show help
show_help() {
    cat << EOF
${WHITE}Redis Rolling Window Monitor${NC}

${YELLOW}USAGE:${NC}
    $0 [command] [options]

${YELLOW}COMMANDS:${NC}
    overview                    Show compact overview of all instruments
    dashboard [INSTRUMENT]      Show detailed dashboard for instrument
    stream [INSTRUMENT] [METRIC] Stream real-time data
    keys                        Show available TimeSeries keys
    help                        Show this help

${YELLOW}EXAMPLES:${NC}
    $0 overview                                    # Market overview
    $0 dashboard TATA_CONSULTANCY_SERVICES        # TCS detailed view
    $0 stream RELIANCE_INDUSTRIES ltp             # Stream Reliance LTP
    $0 keys                                        # Show all available keys

${YELLOW}AVAILABLE INSTRUMENTS:${NC}$(
    for instrument in "${!SYMBOLS[@]}"; do
        echo "    ${SYMBOLS[$instrument]} ($instrument)"
    done
)

${YELLOW}AVAILABLE METRICS:${NC}$(
    for metric in "${METRICS[@]}"; do
        echo "    $metric"
    done
)

${YELLOW}ENVIRONMENT VARIABLES:${NC}
    REDIS_HOST              Redis hostname (default: localhost)
    REDIS_PORT              Redis port (default: 6379)
    REDIS_DB                Redis database (default: 1)
    REFRESH_INTERVAL        Refresh interval in seconds (default: 2)

EOF
}

# Function to continuously monitor with auto-refresh
auto_refresh_overview() {
    while true; do
        clear
        show_overview
        echo -e "\n${CYAN}🔄 Auto-refreshing every ${REFRESH_INTERVAL}s (Press Ctrl+C to stop)${NC}"
        sleep "$REFRESH_INTERVAL"
    done
}

# Main function
main() {
    local command="${1:-overview}"
    
    case "$command" in
        "overview")
            check_redis_connection
            if [[ "${2:-}" == "auto" ]]; then
                auto_refresh_overview
            else
                show_overview
            fi
            ;;
        "dashboard")
            check_redis_connection
            local instrument="${2:-TATA_CONSULTANCY_SERVICES}"
            if [[ ! -v "SYMBOLS[$instrument]" ]]; then
                echo -e "${RED}❌ Unknown instrument: $instrument${NC}"
                echo -e "${YELLOW}Available instruments:${NC}"
                for inst in "${!SYMBOLS[@]}"; do
                    echo "  ${SYMBOLS[$inst]} ($inst)"
                done
                exit 1
            fi
            show_instrument_dashboard "$instrument"
            ;;
        "stream")
            check_redis_connection
            local instrument="${2:-TATA_CONSULTANCY_SERVICES}"
            local metric="${3:-ltp}"
            stream_data "$instrument" "$metric"
            ;;
        "keys")
            check_redis_connection
            show_available_keys
            ;;
        "help"|"--help"|"-h")
            show_help
            ;;
        *)
            echo -e "${RED}❌ Unknown command: $command${NC}"
            show_help
            exit 1
            ;;
    esac
}

# Check dependencies
command -v redis-cli >/dev/null 2>&1 || {
    echo -e "${RED}❌ redis-cli is required but not installed.${NC}"
    echo -e "${YELLOW}Install with: sudo apt-get install redis-tools${NC}"
    exit 1
}

command -v bc >/dev/null 2>&1 || {
    echo -e "${RED}❌ bc is required but not installed.${NC}"
    echo -e "${YELLOW}Install with: sudo apt-get install bc${NC}"
    exit 1
}

# Run main function with all arguments
main "$@"
