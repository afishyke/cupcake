#!/bin/bash

# CUPCAKE Technical Indicator Viewer
# Simple bash script to view indicator values in row/column format

# Colors for better visualization
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
WHITE='\033[1;37m'
NC='\033[0m' # No Color

# Redis connection
REDIS_DB=2
REDIS_HOST="localhost"
REDIS_PORT=6379

# Function to print header
print_header() {
    echo -e "${WHITE}=================================================="
    echo -e "  CUPCAKE TECHNICAL INDICATOR VIEWER"
    echo -e "  Redis DB${REDIS_DB} | $(date '+%Y-%m-%d %H:%M:%S')"
    echo -e "==================================================${NC}"
}

# Function to get latest indicator value
get_indicator_value() {
    local symbol=$1
    local category=$2
    local indicator=$3
    
    local key="indicator:${symbol}:${category}:${indicator}"
    local result=$(redis-cli -h $REDIS_HOST -p $REDIS_PORT -n $REDIS_DB TS.GET "$key" 2>/dev/null)
    
    if [[ -n "$result" && "$result" != "(empty array)" ]]; then
        # Extract value from result (timestamp value)
        local value=$(echo "$result" | awk '{print $2}')
        echo "$value"
    else
        echo "N/A"
    fi
}

# Function to get timestamp of latest value
get_indicator_timestamp() {
    local symbol=$1
    local category=$2
    local indicator=$3
    
    local key="indicator:${symbol}:${category}:${indicator}"
    local result=$(redis-cli -h $REDIS_HOST -p $REDIS_PORT -n $REDIS_DB TS.GET "$key" 2>/dev/null)
    
    if [[ -n "$result" && "$result" != "(empty array)" ]]; then
        # Extract timestamp from result
        local timestamp_ms=$(echo "$result" | awk '{print $1}')
        if [[ "$timestamp_ms" != "" ]]; then
            # Convert milliseconds to readable format
            local timestamp_sec=$((timestamp_ms / 1000))
            date -d "@$timestamp_sec" '+%H:%M:%S'
        else
            echo "N/A"
        fi
    else
        echo "N/A"
    fi
}

# Function to format number for display
format_number() {
    local num=$1
    if [[ "$num" == "N/A" ]]; then
        echo "     N/A"
    elif [[ $(echo "$num" | grep -E '^-?[0-9]+\.?[0-9]*$') ]]; then
        printf "%8.2f" "$num"
    else
        echo "     N/A"
    fi
}

# Function to colorize RSI values
colorize_rsi() {
    local value=$1
    if [[ "$value" == "N/A" ]]; then
        echo -e "${WHITE}     N/A${NC}"
    elif (( $(echo "$value < 30" | bc -l 2>/dev/null || echo "0") )); then
        echo -e "${GREEN}$(format_number $value)${NC}"  # Oversold - Green
    elif (( $(echo "$value > 70" | bc -l 2>/dev/null || echo "0") )); then
        echo -e "${RED}$(format_number $value)${NC}"    # Overbought - Red
    else
        echo -e "${WHITE}$(format_number $value)${NC}"   # Neutral - White
    fi
}

# Function to get all available symbols
get_symbols() {
    redis-cli -h $REDIS_HOST -p $REDIS_PORT -n $REDIS_DB KEYS "indicator:*:trend:sma_20" 2>/dev/null | \
    sed 's/indicator://g' | \
    sed 's/:trend:sma_20//g' | \
    sort
}

# Function to display trend indicators
show_trend_indicators() {
    local symbol=$1
    
    echo -e "\n${CYAN}📈 TREND INDICATORS for $symbol${NC}"
    echo "────────────────────────────────────────────────"
    printf "%-12s %-10s %-10s %-10s %-10s %-10s %-10s\n" \
           "Indicator" "SMA-5" "SMA-20" "SMA-50" "EMA-20" "MACD" "Time"
    echo "────────────────────────────────────────────────"
    
    local sma5=$(get_indicator_value "$symbol" "trend" "sma_5")
    local sma20=$(get_indicator_value "$symbol" "trend" "sma_20")
    local sma50=$(get_indicator_value "$symbol" "trend" "sma_50")
    local ema20=$(get_indicator_value "$symbol" "trend" "ema_20")
    local macd=$(get_indicator_value "$symbol" "trend" "macd")
    local time=$(get_indicator_timestamp "$symbol" "trend" "sma_20")
    
    printf "%-12s " "Values"
    printf "%10s " "$(format_number $sma5)"
    printf "%10s " "$(format_number $sma20)"
    printf "%10s " "$(format_number $sma50)"
    printf "%10s " "$(format_number $ema20)"
    printf "%10s " "$(format_number $macd)"
    printf "%10s\n" "$time"
}

# Function to display momentum indicators
show_momentum_indicators() {
    local symbol=$1
    
    echo -e "\n${YELLOW}📊 MOMENTUM INDICATORS for $symbol${NC}"
    echo "────────────────────────────────────────────────"
    printf "%-12s %-10s %-10s %-10s %-10s %-10s\n" \
           "Indicator" "RSI" "Stoch-K" "CCI" "Will%R" "Time"
    echo "────────────────────────────────────────────────"
    
    local rsi=$(get_indicator_value "$symbol" "momentum" "rsi")
    local stoch_k=$(get_indicator_value "$symbol" "momentum" "stoch_k")
    local cci=$(get_indicator_value "$symbol" "momentum" "cci")
    local williams_r=$(get_indicator_value "$symbol" "momentum" "williams_r")
    local time=$(get_indicator_timestamp "$symbol" "momentum" "rsi")
    
    printf "%-12s " "Values"
    printf "%10s " "$(colorize_rsi $rsi)"
    printf "%10s " "$(format_number $stoch_k)"
    printf "%10s " "$(format_number $cci)"
    printf "%10s " "$(format_number $williams_r)"
    printf "%10s\n" "$time"
}

# Function to display volume indicators
show_volume_indicators() {
    local symbol=$1
    
    echo -e "\n${PURPLE}📈 VOLUME INDICATORS for $symbol${NC}"
    echo "────────────────────────────────────────────────"
    printf "%-12s %-12s %-12s %-10s %-10s\n" \
           "Indicator" "Vol-SMA-20" "OBV" "Vol-Ratio" "Time"
    echo "────────────────────────────────────────────────"
    
    local vol_sma20=$(get_indicator_value "$symbol" "volume" "volume_sma_20")
    local obv=$(get_indicator_value "$symbol" "volume" "obv")
    local vol_ratio=$(get_indicator_value "$symbol" "volume" "volume_ratio")
    local time=$(get_indicator_timestamp "$symbol" "volume" "obv")
    
    printf "%-12s " "Values"
    printf "%12s " "$(format_number $vol_sma20)"
    printf "%12s " "$(format_number $obv)"
    printf "%10s " "$(format_number $vol_ratio)"
    printf "%10s\n" "$time"
}

# Function to display volatility indicators
show_volatility_indicators() {
    local symbol=$1
    
    echo -e "\n${BLUE}📊 VOLATILITY INDICATORS for $symbol${NC}"
    echo "────────────────────────────────────────────────"
    printf "%-12s %-10s %-10s %-10s %-10s\n" \
           "Indicator" "ATR" "ATR%" "StdDev" "Time"
    echo "────────────────────────────────────────────────"
    
    local atr=$(get_indicator_value "$symbol" "volatility" "atr")
    local atr_percent=$(get_indicator_value "$symbol" "volatility" "atr_percent")
    local std_dev=$(get_indicator_value "$symbol" "volatility" "std_dev")
    local time=$(get_indicator_timestamp "$symbol" "volatility" "atr")
    
    printf "%-12s " "Values"
    printf "%10s " "$(format_number $atr)"
    printf "%10s " "$(format_number $atr_percent)"
    printf "%10s " "$(format_number $std_dev)"
    printf "%10s\n" "$time"
}

# Function to show quick summary for all symbols
show_quick_summary() {
    echo -e "\n${WHITE}📋 QUICK SUMMARY - Key Indicators${NC}"
    echo "═══════════════════════════════════════════════════════════════════════"
    printf "%-12s %-8s %-8s %-8s %-8s %-8s %-8s %-8s\n" \
           "Symbol" "RSI" "SMA20" "SMA50" "MACD" "ATR%" "VolRat" "Time"
    echo "───────────────────────────────────────────────────────────────────────"
    
    local symbols=($(get_symbols))
    
    for symbol in "${symbols[@]}"; do
        if [[ -n "$symbol" ]]; then
            local rsi=$(get_indicator_value "$symbol" "momentum" "rsi")
            local sma20=$(get_indicator_value "$symbol" "trend" "sma_20")
            local sma50=$(get_indicator_value "$symbol" "trend" "sma_50")
            local macd=$(get_indicator_value "$symbol" "trend" "macd")
            local atr_percent=$(get_indicator_value "$symbol" "volatility" "atr_percent")
            local vol_ratio=$(get_indicator_value "$symbol" "volume" "volume_ratio")
            local time=$(get_indicator_timestamp "$symbol" "momentum" "rsi")
            
            printf "%-12s " "$symbol"
            printf "%-8s " "$(format_number $rsi | tr -d ' ')"
            printf "%-8s " "$(format_number $sma20 | tr -d ' ')"
            printf "%-8s " "$(format_number $sma50 | tr -d ' ')"
            printf "%-8s " "$(format_number $macd | tr -d ' ')"
            printf "%-8s " "$(format_number $atr_percent | tr -d ' ')"
            printf "%-8s " "$(format_number $vol_ratio | tr -d ' ')"
            printf "%-8s\n" "$time"
        fi
    done
}

# Function to show specific symbol details
show_symbol_details() {
    local symbol=$1
    
    echo -e "\n${WHITE}🎯 DETAILED VIEW for $symbol${NC}"
    show_trend_indicators "$symbol"
    show_momentum_indicators "$symbol"
    show_volume_indicators "$symbol"
    show_volatility_indicators "$symbol"
}

# Function to show usage
show_usage() {
    echo -e "${WHITE}Usage:${NC}"
    echo "  $0                    - Show quick summary for all symbols"
    echo "  $0 <symbol>          - Show detailed view for specific symbol"
    echo "  $0 list              - List all available symbols"
    echo "  $0 trend <symbol>    - Show only trend indicators"
    echo "  $0 momentum <symbol> - Show only momentum indicators"
    echo "  $0 volume <symbol>   - Show only volume indicators"
    echo "  $0 volatility <symbol> - Show only volatility indicators"
    echo ""
    echo -e "${WHITE}Examples:${NC}"
    echo "  $0                   # Quick view of all symbols"
    echo "  $0 RELIANCE         # Detailed view for RELIANCE"
    echo "  $0 trend TCS        # Only trend indicators for TCS"
    echo "  $0 list             # List available symbols"
}

# Function to check Redis connection
check_redis_connection() {
    if ! redis-cli -h $REDIS_HOST -p $REDIS_PORT -n $REDIS_DB ping >/dev/null 2>&1; then
        echo -e "${RED}❌ Error: Cannot connect to Redis DB${REDIS_DB} at ${REDIS_HOST}:${REDIS_PORT}${NC}"
        echo -e "${WHITE}Make sure Redis is running and technical indicators are populated.${NC}"
        exit 1
    fi
}

# Function to list available symbols
list_symbols() {
    echo -e "\n${WHITE}📋 Available Symbols:${NC}"
    echo "────────────────────────"
    local symbols=($(get_symbols))
    local count=0
    
    for symbol in "${symbols[@]}"; do
        if [[ -n "$symbol" ]]; then
            printf "%-15s" "$symbol"
            ((count++))
            if (( count % 4 == 0 )); then
                echo ""
            fi
        fi
    done
    
    if (( count % 4 != 0 )); then
        echo ""
    fi
    
    echo "────────────────────────"
    echo -e "${WHITE}Total: $count symbols${NC}"
}

# Main script logic
main() {
    # Check command line arguments
    case "$1" in
        "")
            # No arguments - show quick summary
            print_header
            check_redis_connection
            show_quick_summary
            ;;
        "list")
            print_header
            check_redis_connection
            list_symbols
            ;;
        "help"|"-h"|"--help")
            print_header
            show_usage
            ;;
        "trend")
            if [[ -z "$2" ]]; then
                echo -e "${RED}Error: Symbol required for trend view${NC}"
                show_usage
                exit 1
            fi
            print_header
            check_redis_connection
            show_trend_indicators "$2"
            ;;
        "momentum")
            if [[ -z "$2" ]]; then
                echo -e "${RED}Error: Symbol required for momentum view${NC}"
                show_usage
                exit 1
            fi
            print_header
            check_redis_connection
            show_momentum_indicators "$2"
            ;;
        "volume")
            if [[ -z "$2" ]]; then
                echo -e "${RED}Error: Symbol required for volume view${NC}"
                show_usage
                exit 1
            fi
            print_header
            check_redis_connection
            show_volume_indicators "$2"
            ;;
        "volatility")
            if [[ -z "$2" ]]; then
                echo -e "${RED}Error: Symbol required for volatility view${NC}"
                show_usage
                exit 1
            fi
            print_header
            check_redis_connection
            show_volatility_indicators "$2"
            ;;
        *)
            # Assume it's a symbol name
            print_header
            check_redis_connection
            show_symbol_details "$1"
            ;;
    esac
}

# Run main function with all arguments
main "$@"