# Makefile for Tick-Tick Stock Monitoring Program Docker Operations

# Variables
PROJECT_NAME = cupcake
COMPOSE_FILE = docker-compose.yml
ENV_FILE = .env

# Colors for output
RED = \033[0;31m
GREEN = \033[0;32m
YELLOW = \033[1;33m
BLUE = \033[0;34m
NC = \033[0m # No Color

.PHONY: help build up down logs status clean install health check-deps setup

# Default target
help: ## Show this help message
	@echo "$(BLUE)Tick-Tick Stock Monitoring Program - Docker Operations$(NC)"
	@echo "======================================================"
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "$(GREEN)%-20s$(NC) %s\n", $$1, $$2}' $(MAKEFILE_LIST)

check-deps: ## Check if required dependencies are installed
	@echo "$(BLUE)Checking dependencies...$(NC)"
	@command -v docker >/dev/null 2>&1 || { echo "$(RED)❌ Docker is not installed$(NC)"; exit 1; }
	@command -v docker-compose >/dev/null 2>&1 || command -v docker compose >/dev/null 2>&1 || { echo "$(RED)❌ Docker Compose is not installed$(NC)"; exit 1; }
	@echo "$(GREEN)✅ All dependencies are installed$(NC)"

setup: check-deps ## Initial setup and configuration
	@echo "$(BLUE)Setting up Tick-Tick monitoring environment...$(NC)"
	@mkdir -p logs authentication scripts web bash_scripts
	@[ -f $(ENV_FILE) ] || cp .env.example $(ENV_FILE) 2>/dev/null || echo "# Tick-Tick Environment Configuration\nREDIS_HOST=redis\nREDIS_PORT=6379\nREDIS_DB=0\nBUFFER_SIZE=600\nOUTLIER_SIGMA=3.0\nTZ=Asia/Kolkata" > $(ENV_FILE)
	@[ -f authentication/credentials.json ] || echo "$(YELLOW)⚠️  Create authentication/credentials.json with your Upstox API credentials$(NC)"
	@echo "$(GREEN)✅ Setup completed$(NC)"

build: check-deps ## Build all Docker images
	@echo "$(BLUE)Building Docker images...$(NC)"
	@docker-compose build
	@echo "$(GREEN)✅ Build completed$(NC)"

rebuild: ## Rebuild all images without cache
	@echo "$(BLUE)Rebuilding Docker images without cache...$(NC)"
	@docker-compose build --no-cache
	@echo "$(GREEN)✅ Rebuild completed$(NC)"

up: ## Start all services
	@echo "$(BLUE)Starting all services...$(NC)"
	@docker-compose up -d
	@echo "$(GREEN)✅ All services started$(NC)"
	@make status

up-build: ## Build and start all services
	@echo "$(BLUE)Building and starting all services...$(NC)"
	@docker-compose up -d --build
	@echo "$(GREEN)✅ All services built and started$(NC)"
	@make status

# Profile-specific targets
historical: ## Run historical data fetcher only
	@echo "$(BLUE)Starting historical data fetcher...$(NC)"
	@docker-compose --profile historical up -d
	@make status

live: ## Run live data fetcher only
	@echo "$(BLUE)Starting live data fetcher...$(NC)"
	@docker-compose --profile live up -d
	@make status

websocket: ## Run WebSocket analytics only
	@echo "$(BLUE)Starting WebSocket analytics...$(NC)"
	@docker-compose --profile websocket up -d
	@make status

monitor: ## Run monitoring dashboard
	@echo "$(BLUE)Starting monitoring dashboard...$(NC)"
	@docker-compose --profile monitor up -d
	@make status

pipeline: ## Run complete data pipeline (historical + live + websocket)
	@echo "$(BLUE)Starting complete data pipeline...$(NC)"
	@docker-compose up -d redis
	@echo "Waiting for Redis to be ready..."
	@sleep 5
	@docker-compose --profile historical up cupcake-historical
	@docker-compose --profile live --profile websocket up -d
	@make status

down: ## Stop all services
	@echo "$(BLUE)Stopping all services...$(NC)"
	@docker-compose down
	@echo "$(GREEN)✅ All services stopped$(NC)"

stop: ## Stop all services (alias for down)
	@make down

restart: ## Restart all services
	@echo "$(BLUE)Restarting all services...$(NC)"
	@docker-compose restart
	@make status

status: ## Show status of all services
	@echo "$(BLUE)Service Status:$(NC)"
	@docker-compose ps

logs: ## Show logs from all services
	@docker-compose logs -f

logs-app: ## Show logs from main application only
	@docker-compose logs -f cupcake-app

logs-redis: ## Show logs from Redis only
	@docker-compose logs -f redis

logs-websocket: ## Show logs from WebSocket service only
	@docker-compose logs -f cupcake-websocket

health: ## Check health of all services
	@echo "$(BLUE)Checking service health...$(NC)"
	@docker-compose exec cupcake-app /app/docker-healthcheck.sh 2>/dev/null || echo "$(YELLOW)⚠️  Main app not running$(NC)"
	@docker-compose exec redis redis-cli ping 2>/dev/null && echo "$(GREEN)✅ Redis: PONG$(NC)" || echo "$(RED)❌ Redis: Not responding$(NC)"

shell: ## Access main application shell
	@echo "$(BLUE)Accessing application shell...$(NC)"
	@docker-compose exec cupcake-app bash

shell-redis: ## Access Redis CLI
	@echo "$(BLUE)Accessing Redis CLI...$(NC)"
	@docker-compose exec redis redis-cli

# Monitoring and debugging
monitor-redis: ## Monitor Redis with custom script
	@docker-compose exec cupcake-app /app/bash_scripts/redis_monitor_script.sh overview auto

dump-data: ## Dump market data to file
	@echo "$(BLUE)Dumping market data...$(NC)"
	@docker-compose exec cupcake-app /app/bash_scripts/dump_hlcv_ist.sh > market_data_$(shell date +%Y%m%d_%H%M%S).txt
	@echo "$(GREEN)✅ Data dumped to market_data_$(shell date +%Y%m%d_%H%M%S).txt$(NC)"

# Maintenance commands
clean: ## Remove stopped containers and unused images
	@echo "$(BLUE)Cleaning up Docker resources...$(NC)"
	@docker-compose down -v
	@docker system prune -f
	@echo "$(GREEN)✅ Cleanup completed$(NC)"

clean-all: ## Remove all containers, images, and volumes (DESTRUCTIVE)
	@echo "$(RED)⚠️  This will remove ALL containers, images, and volumes!$(NC)"
	@read -p "Are you sure? [y/N] " confirm && [ "$$confirm" = "y" ] || exit 1
	@docker-compose down -v --rmi all
	@docker system prune -a -f --volumes
	@echo "$(GREEN)✅ Complete cleanup finished$(NC)"

backup: ## Backup Redis data and logs
	@echo "$(BLUE)Creating backup...$(NC)"
	@mkdir -p backups
	@docker-compose exec redis redis-cli BGSAVE
	@sleep 2
	@docker cp $$(docker-compose ps -q redis):/data/dump.rdb ./backups/redis_backup_$(shell date +%Y%m%d_%H%M%S).rdb
	@tar -czf backups/logs_backup_$(shell date +%Y%m%d_%H%M%S).tar.gz logs/
	@echo "$(GREEN)✅ Backup completed in backups/ directory$(NC)"

# Development helpers
dev: ## Start development environment (Redis only)
	@echo "$(BLUE)Starting development environment...$(NC)"
	@docker-compose up -d redis
	@echo "$(GREEN)✅ Redis started for development$(NC)"
	@echo "Run scripts manually with: docker-compose run --rm cupcake-app python scripts/script_name.py"

test: ## Run tests (if available)
	@echo "$(BLUE)Running tests...$(NC)"
	@docker-compose run --rm cupcake-app python -m pytest tests/ || echo "$(YELLOW)⚠️  No tests found or pytest not available$(NC)"

install: setup build ## Complete installation (setup + build)
	@echo "$(GREEN)✅ Installation completed$(NC)"
	@echo "$(BLUE)Next steps:$(NC)"
	@echo "1. Update authentication/credentials.json with your Upstox API credentials"
	@echo "2. Run: make up-build"
	@echo "3. Monitor with: make logs"

# Production helpers
deploy: ## Deploy in production mode
	@echo "$(BLUE)Deploying in production mode...$(NC)"
	@docker-compose -f docker-compose.yml -f docker-compose.prod.yml up -d --build
	@make status

# Information commands
info: ## Show system information
	@echo "$(BLUE)System Information:$(NC)"
	@echo "Docker version: $$(docker --version)"
	@echo "Docker Compose version: $$(docker-compose --version 2>/dev/null || docker compose version)"
	@echo "Available profiles: historical, live, websocket, monitor"
	@echo "Log directory: ./logs"
	@echo "Redis UI: http://localhost:8001"
	@echo "Web interface: http://localhost:8080"

urls: ## Show important URLs
	@echo "$(BLUE)Important URLs:$(NC)"
	@echo "$(GREEN)Redis UI (RedisInsight):$(NC) http://localhost:8001"
	@echo "$(GREEN)Web Interface:$(NC) http://localhost:8080"
	@echo "$(GREEN)Logs Directory:$(NC) ./logs/"

# Quick operations
quick-start: setup build up ## Quick start (setup + build + up)
	@echo "$(GREEN)🚀 Quick start completed!$(NC)"
	@make urls

full-restart: down clean build up ## Full restart with cleanup
	@echo "$(GREEN)✅ Full restart completed$(NC)"