#!/bin/bash
#
# Tick-Tick Stock Monitoring Program - Easy Startup Script
# This script provides an interactive way to start the application
#

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
WHITE='\033[1;37m'
NC='\033[0m' # No Color

# Configuration
PROJECT_NAME="Tick-Tick Stock Monitoring Program"
REQUIRED_FILES=(
    "docker-compose.yml"
    "Dockerfile"
    "requirements.txt"
    "scripts/symbols.json"
)

# Function to print banner
print_banner() {
    echo -e "${BLUE}"
    echo "================================================================================"
    echo "🎯           TICK-TICK STOCK MONITORING PROGRAM STARTUP                      🎯"
    echo "================================================================================"
    echo -e "${NC}"
    echo -e "${CYAN}Real-time market surveillance with AI-powered risk management${NC}"
    echo ""
}

# Function to check prerequisites
check_prerequisites() {
    echo -e "${YELLOW}📋 Checking prerequisites...${NC}"
    
    local missing_deps=0
    
    # Check Docker
    if command -v docker >/dev/null 2>&1; then
        echo -e "${GREEN}✅ Docker: $(docker --version)${NC}"
    else
        echo -e "${RED}❌ Docker: Not installed${NC}"
        missing_deps=1
    fi
    
    # Check Docker Compose
    if command -v docker-compose >/dev/null 2>&1; then
        echo -e "${GREEN}✅ Docker Compose: $(docker-compose --version)${NC}"
    elif command -v docker compose >/dev/null 2>&1; then
        echo -e "${GREEN}✅ Docker Compose: $(docker compose version)${NC}"
    else
        echo -e "${RED}❌ Docker Compose: Not installed${NC}"
        missing_deps=1
    fi
    
    # Check required files
    for file in "${REQUIRED_FILES[@]}"; do
        if [ -f "$file" ]; then
            echo -e "${GREEN}✅ File: $file${NC}"
        else
            echo -e "${RED}❌ File: $file (missing)${NC}"
            missing_deps=1
        fi
    done
    
    # Check credentials
    if [ -f "authentication/credentials.json" ]; then
        echo -e "${GREEN}✅ Credentials: authentication/credentials.json${NC}"
    else
        echo -e "${YELLOW}⚠️  Credentials: authentication/credentials.json (missing)${NC}"
        echo -e "${YELLOW}   You'll need to create this file with your Upstox API credentials${NC}"
    fi
    
    # Check logs directory
    mkdir -p logs
    if [ -d "logs" ] && [ -w "logs" ]; then
        echo -e "${GREEN}✅ Logs directory: logs/ (writable)${NC}"
    else
        echo -e "${RED}❌ Logs directory: Not writable${NC}"
        missing_deps=1
    fi
    
    if [ $missing_deps -ne 0 ]; then
        echo -e "\n${RED}❌ Prerequisites check failed. Please install missing dependencies.${NC}"
        exit 1
    fi
    
    echo -e "\n${GREEN}✅ All prerequisites satisfied!${NC}\n"
}

# Function to show menu
show_menu() {
    echo -e "${WHITE}🚀 STARTUP OPTIONS:${NC}"
    echo -e "${WHITE}==================${NC}"
    echo ""
    echo -e "${GREEN}1)${NC} 🏗️  Quick Start (Build + Start All Services)"
    echo -e "${GREEN}2)${NC} 📊 Data Pipeline (Historical + Live + Analytics)"
    echo -e "${GREEN}3)${NC} 📈 Historical Data Only"
    echo -e "${GREEN}4)${NC} 📡 Live Data Streaming Only"  
    echo -e "${GREEN}5)${NC} 🌐 WebSocket Analytics Only"
    echo -e "${GREEN}6)${NC} 📋 Redis Monitoring Dashboard"
    echo -e "${GREEN}7)${NC} 🛠️  Development Mode (Redis Only)"
    echo -e "${GREEN}8)${NC} 🔍 Check Service Status"
    echo -e "${GREEN}9)${NC} 📜 View Logs"
    echo -e "${RED}10)${NC} 🛑 Stop All Services"
    echo -e "${RED}11)${NC} 🧹 Clean Up (Remove Containers)"
    echo ""
    echo -e "${BLUE}0)${NC} ❓ Show Help & Information"
    echo ""
    echo -e "${YELLOW}q)${NC} 🚪 Quit"
    echo ""
}

# Function to show help
show_help() {
    echo -e "${BLUE}📚 HELP & INFORMATION${NC}"
    echo -e "${BLUE}====================${NC}"
    echo ""
    echo -e "${WHITE}Architecture Overview:${NC}"
    echo -e "${GREEN}• Redis:${NC} TimeSeries database for market data storage"
    echo -e "${GREEN}• Historical Fetcher:${NC} Downloads 250 recent 1-minute OHLCV candles"
    echo -e "${GREEN}• Live Fetcher:${NC} Continuously updates with new market data"
    echo -e "${GREEN}• WebSocket Analytics:${NC} Real-time technical analysis engine"
    echo -e "${GREEN}• Monitoring:${NC} Dashboard for data visualization"
    echo ""
    echo -e "${WHITE}Important URLs:${NC}"
    echo -e "${GREEN}• Redis UI:${NC} http://localhost:8001"
    echo -e "${GREEN}• Web Interface:${NC} http://localhost:8080" 
    echo -e "${GREEN}• Logs Directory:${NC} ./logs/"
    echo ""
    echo -e "${WHITE}Quick Commands:${NC}"
    echo -e "${GREEN}• View Redis data:${NC} make shell-redis"
    echo -e "${GREEN}• Application shell:${NC} make shell"
    echo -e "${GREEN}• Export data:${NC} make dump-data"
    echo -e "${GREEN}• Full restart:${NC} make full-restart"
    echo ""
    echo -e "${WHITE}Configuration:${NC}"
    echo -e "${GREEN}• Credentials:${NC} authentication/credentials.json"
    echo -e "${GREEN}• Symbols:${NC} scripts/symbols.json"
    echo -e "${GREEN}• Environment:${NC} .env file"
    echo ""
    read -p "Press Enter to continue..."
}

# Function to wait for service
wait_for_service() {
    local service_name=$1
    local max_attempts=30
    local attempt=1
    
    echo -e "${YELLOW}⏳ Waiting for $service_name to be ready...${NC}"
    
    while [ $attempt -le $max_attempts ]; do
        if docker-compose ps $service_name | grep -q "Up"; then
            echo -e "${GREEN}✅ $service_name is ready!${NC}"
            return 0
        fi
        echo -n "."
        sleep 2
        ((attempt++))
    done
    
    echo -e "\n${RED}❌ $service_name failed to start within expected time${NC}"
    return 1
}

# Function to show service status
show_status() {
    echo -e "${BLUE}📊 SERVICE STATUS${NC}"
    echo -e "${BLUE}================${NC}"
    docker-compose ps
    echo ""
    
    # Check Redis specifically
    if docker-compose exec redis redis-cli ping >/dev/null 2>&1; then
        echo -e "${GREEN}✅ Redis: Responding to ping${NC}"
        
        # Check for TimeSeries data
        local ts_count=$(docker-compose exec redis redis-cli KEYS "stock:*" 2>/dev/null | wc -l)
        echo -e "${GREEN}📊 TimeSeries keys: $ts_count${NC}"
    else
        echo -e "${RED}❌ Redis: Not responding${NC}"
    fi
    
    echo ""
    read -p "Press Enter to continue..."
}

# Function to view logs
view_logs() {
    echo -e "${BLUE}📜 LOG VIEWER${NC}"
    echo -e "${BLUE}=============${NC}"
    echo ""
    echo "1) All services"
    echo "2) Main application"
    echo "3) Redis"
    echo "4) WebSocket analytics"
    echo "5) Live data fetcher"
    echo ""
    read -p "Select log source (1-5): " log_choice
    
    case $log_choice in
        1)
            echo -e "${YELLOW}Showing logs from all services (Ctrl+C to stop)...${NC}"
            docker-compose logs -f
            ;;
        2)
            echo -e "${YELLOW}Showing main application logs (Ctrl+C to stop)...${NC}"
            docker-compose logs -f cupcake-app
            ;;
        3)
            echo -e "${YELLOW}Showing Redis logs (Ctrl+C to stop)...${NC}"
            docker-compose logs -f redis
            ;;
        4)
            echo -e "${YELLOW}Showing WebSocket analytics logs (Ctrl+C to stop)...${NC}"
            docker-compose logs -f cupcake-websocket
            ;;
        5)
            echo -e "${YELLOW}Showing live data fetcher logs (Ctrl+C to stop)...${NC}"
            docker-compose logs -f cupcake-live
            ;;
        *)
            echo -e "${RED}Invalid selection${NC}"
            ;;
    esac
}

# Function to execute user choice
execute_choice() {
    local choice=$1
    
    case $choice in
        1)
            echo -e "${BLUE}🏗️ Starting Quick Start...${NC}"
            docker-compose up -d --build
            wait_for_service redis
            echo -e "${GREEN}✅ All services started successfully!${NC}"
            echo -e "${CYAN}🌐 Access Redis UI at: http://localhost:8001${NC}"
            echo -e "${CYAN}🌐 Access Web Interface at: http://localhost:8080${NC}"
            ;;
            
        2)
            echo -e "${BLUE}📊 Starting Data Pipeline...${NC}"
            echo -e "${YELLOW}Step 1: Starting Redis...${NC}"
            docker-compose up -d redis
            wait_for_service redis
            
            echo -e "${YELLOW}Step 2: Running historical data fetch...${NC}"
            docker-compose --profile historical up cupcake-historical
            
            echo -e "${YELLOW}Step 3: Starting live data and analytics...${NC}"
            docker-compose --profile live --profile websocket up -d
            
            echo -e "${GREEN}✅ Complete data pipeline is running!${NC}"
            ;;
            
        3)
            echo -e "${BLUE}📈 Starting Historical Data Fetcher...${NC}"
            docker-compose up -d redis
            wait_for_service redis
            docker-compose --profile historical up cupcake-historical
            ;;
            
        4)
            echo -e "${BLUE}📡 Starting Live Data Streaming...${NC}"
            docker-compose up -d redis
            wait_for_service redis
            docker-compose --profile live up -d cupcake-live
            echo -e "${GREEN}✅ Live data streaming started!${NC}"
            ;;
            
        5)
            echo -e "${BLUE}🌐 Starting WebSocket Analytics...${NC}"
            docker-compose up -d redis
            wait_for_service redis
            docker-compose --profile websocket up -d cupcake-websocket
            echo -e "${GREEN}✅ WebSocket analytics started!${NC}"
            ;;
            
        6)
            echo -e "${BLUE}📋 Starting Redis Monitoring...${NC}"
            docker-compose up -d redis
            wait_for_service redis
            docker-compose --profile monitor up cupcake-monitor
            ;;
            
        7)
            echo -e "${BLUE}🛠️ Starting Development Mode...${NC}"
            docker-compose up -d redis
            wait_for_service redis
            echo -e "${GREEN}✅ Development environment ready!${NC}"
            echo -e "${CYAN}Run scripts with: docker-compose run --rm cupcake-app python scripts/script_name.py${NC}"
            ;;
            
        8)
            show_status
            ;;
            
        9)
            view_logs
            ;;
            
        10)
            echo -e "${RED}🛑 Stopping all services...${NC}"
            docker-compose down
            echo -e "${GREEN}✅ All services stopped${NC}"
            ;;
            
        11)
            echo -e "${RED}🧹 Cleaning up...${NC}"
            docker-compose down -v
            docker system prune -f
            echo -e "${GREEN}✅ Cleanup completed${NC}"
            ;;
            
        0)
            show_help
            ;;
            
        q|Q)
            echo -e "${BLUE}👋 Goodbye!${NC}"
            exit 0
            ;;
            
        *)
            echo -e "${RED}❌ Invalid option. Please try again.${NC}"
            sleep 2
            ;;
    esac
}

# Main function
main() {
    print_banner
    check_prerequisites
    
    while true; do
        clear
        print_banner
        show_menu
        
        read -p "$(echo -e ${WHITE}Choose an option: ${NC})" choice
        echo ""
        
        execute_choice "$choice"
        
        if [[ "$choice" != "0" && "$choice" != "8" && "$choice" != "9" && "$choice" != "q" && "$choice" != "Q" ]]; then
            echo ""
            read -p "Press Enter to return to menu..."
        fi
    done
}

# Check if we're in the right directory
if [ ! -f "docker-compose.yml" ]; then
    echo -e "${RED}❌ Error: docker-compose.yml not found in current directory${NC}"
    echo -e "${YELLOW}Please run this script from the project root directory${NC}"
    exit 1
fi

# Run main function
main "$@"