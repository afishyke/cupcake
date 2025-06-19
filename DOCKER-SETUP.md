# 🐳 Docker Setup for Tick-Tick Stock Monitoring Program

This document provides comprehensive instructions for running the Tick-Tick Stock Monitoring Program using Docker and Docker Compose.

## 📋 Prerequisites

### Required Software
- **Docker** (20.10+): [Install Docker](https://docs.docker.com/get-docker/)
- **Docker Compose** (2.0+): [Install Docker Compose](https://docs.docker.com/compose/install/)
- **Git**: For cloning the repository

### System Requirements
- **Memory**: Minimum 4GB RAM (8GB+ recommended for production)
- **Disk Space**: At least 10GB free space
- **Network**: Stable internet connection for market data

### Required Files
Before building, ensure you have:
- `authentication/credentials.json` with valid Upstox API credentials
- All Python scripts in the `scripts/` directory
- Protocol buffer files (`.proto`)

## 🚀 Quick Start

### 1. Clone and Prepare
```bash
# Clone the repository
git clone <your-repo-url>
cd tick-tick-stock-monitor

# Ensure credentials file exists
cp authentication/credentials.json.template authentication/credentials.json
# Edit credentials.json with your actual Upstox API credentials
```

### 2. Build and Run
```bash
# Build and start all services
docker-compose up --build

# Or run in background
docker-compose up -d --build
```

### 3. Verify Setup
```bash
# Check service status
docker-compose ps

# View logs
docker-compose logs -f cupcake-app

# Check Redis connectivity
docker-compose exec redis redis-cli ping
```

## 🏗️ Architecture Overview

### Services

| Service | Description | Port | Database |
|---------|-------------|------|----------|
| `redis` | Redis with TimeSeries module | 6379, 8001 | - |
| `cupcake-app` | Main application (default) | 8080 | DB 0 |
| `cupcake-historical` | Historical data fetcher | - | DB 0 |
| `cupcake-live` | Live data fetcher | - | DB 0 |
| `cupcake-websocket` | Real-time analytics | - | DB 1 |
| `cupcake-monitor` | Monitoring dashboard | - | DB 1 |

### Profiles

Use Docker Compose profiles to run specific components:

```bash
# Run only historical data fetcher
docker-compose --profile historical up

# Run live data fetcher
docker-compose --profile live up

# Run WebSocket analytics
docker-compose --profile websocket up

# Run monitoring
docker-compose --profile monitor up

# Run multiple profiles
docker-compose --profile live --profile websocket up
```

## 🔧 Configuration

### Environment Variables

Create a `.env` file in the project root:

```bash
# Redis Configuration
REDIS_HOST=redis
REDIS_PORT=6379
REDIS_DB=0

# Application Configuration
BUFFER_SIZE=600
OUTLIER_SIGMA=3.0
CUPCAKE_LOG_DIR=/app/logs

# Timezone
TZ=Asia/Kolkata

# Performance Tuning
REFRESH_INTERVAL=2
```

### Volume Mounts

| Host Path | Container Path | Purpose |
|-----------|----------------|---------|
| `./logs` | `/app/logs` | Application logs |
| `./authentication/credentials.json` | `/app/authentication/credentials.json` | API credentials |
| `redis_data` | `/data` | Redis persistence |
| `app_data` | `/app/data` | Application data |

## 📊 Usage Examples

### 1. Full Pipeline (Historical + Live)
```bash
# Start Redis first
docker-compose up -d redis

# Wait for Redis to be ready
docker-compose exec redis redis-cli ping

# Run historical data fetch once
docker-compose --profile historical up cupcake-historical

# Start live data streaming
docker-compose --profile live up -d cupcake-live

# Start real-time analytics
docker-compose --profile websocket up -d cupcake-websocket
```

### 2. Development Mode
```bash
# Start only Redis
docker-compose up -d redis

# Run specific script interactively
docker-compose run --rm cupcake-app python scripts/historical_data_fetcher.py

# Access container shell
docker-compose exec cupcake-app bash
```

### 3. Monitoring and Debugging
```bash
# View live logs from all services
docker-compose logs -f

# Monitor specific service
docker-compose logs -f cupcake-websocket

# Check Redis data
docker-compose exec redis redis-cli
# > KEYS stock:*
# > TS.INFO stock:Reliance_Industries:close

# Monitor system resources
docker-compose exec cupcake-app ps aux
docker-compose exec cupcake-app df -h
```

### 4. Data Analysis
```bash
# Access Redis monitoring
docker-compose --profile monitor up cupcake-monitor

# Or use the bash script directly
docker-compose exec cupcake-app /app/bash_scripts/redis_monitor_script.sh overview

# Export data
docker-compose exec cupcake-app /app/bash_scripts/dump_hlcv_ist.sh > market_data.txt
```

## 🔍 Health Checks and Monitoring

### Health Check Status
```bash
# Check health of all services
docker-compose ps

# View detailed health check
docker-compose exec cupcake-app /app/docker-healthcheck.sh
```

### Redis Monitoring
- **Redis UI**: Access at `http://localhost:8001` (RedisInsight)
- **Redis CLI**: `docker-compose exec redis redis-cli`
- **TimeSeries Commands**:
  ```bash
  # List all TimeSeries keys
  TS.QUERYINDEX symbol=Reliance_Industries
  
  # Get latest data point
  TS.GET stock:Reliance_Industries:close
  
  # Get time range
  TS.RANGE stock:Reliance_Industries:close - + AGGREGATION avg 60000
  ```

### Application Monitoring
```bash
# View application metrics
docker-compose exec cupcake-app python -c "
import redis
r = redis.Redis(host='redis', port=6379, db=1)
print('Analytics keys:', len(r.keys('ts:*')))
"

# Check log files
docker-compose exec cupcake-app ls -la /app/logs/
docker-compose exec cupcake-app tail -f /app/logs/enhanced_historical_data_fetcher_*.log
```

## 🛠️ Troubleshooting

### Common Issues

#### 1. Redis Connection Failed
```bash
# Check Redis service
docker-compose ps redis

# Restart Redis
docker-compose restart redis

# Check Redis logs
docker-compose logs redis
```

#### 2. Authentication Errors
```bash
# Verify credentials file
docker-compose exec cupcake-app cat /app/authentication/credentials.json

# Test authentication
docker-compose exec cupcake-app python -c "
from authentication.upstox_client import UpstoxAuthClient
client = UpstoxAuthClient()
print('Token valid:', client.is_token_valid())
"
```

#### 3. Missing Protocol Buffer Files
```bash
# Recompile protobuf
docker-compose exec cupcake-app bash -c "
cd scripts && protoc --python_out=. MarketDataFeed.proto
"
```

#### 4. Memory Issues
```bash
# Check memory usage
docker stats

# Increase memory limits in docker-compose.yml
# Add under service definition:
# deploy:
#   resources:
#     limits:
#       memory: 2G
```

#### 5. Port Conflicts
```bash
# Check port usage
netstat -tlnp | grep :6379

# Change ports in docker-compose.yml if needed
```

### Log Analysis
```bash
# Search for errors
docker-compose logs | grep -i error

# Monitor live logs with filtering
docker-compose logs -f | grep -E "(ERROR|WARN|CRITICAL)"

# Export logs for analysis
docker-compose logs > full_application_logs.txt
```

## 🔧 Customization

### Modifying Build Configuration

#### 1. Custom Requirements
Edit `requirements.txt` to add/remove Python packages:
```bash
# Rebuild after changes
docker-compose build --no-cache cupcake-app
```

#### 2. Environment Specific Configs
Create environment-specific compose files:
```bash
# docker-compose.prod.yml
version: '3.8'
services:
  cupcake-app:
    environment:
      - BUFFER_SIZE=1200  # Larger buffer for production
      - OUTLIER_SIGMA=2.5  # Stricter outlier detection

# Use with:
docker-compose -f docker-compose.yml -f docker-compose.prod.yml up
```

#### 3. Resource Limits
```yaml
services:
  cupcake-app:
    deploy:
      resources:
        limits:
          cpus: '2.0'
          memory: 4G
        reservations:
          cpus: '1.0'
          memory: 2G
```

### Adding New Services
```yaml
services:
  cupcake-alerting:
    build: .
    command: ["python", "scripts/alerting_service.py"]
    environment:
      - REDIS_HOST=redis
      - ALERT_WEBHOOK_URL=${WEBHOOK_URL}
    depends_on:
      - redis
    profiles:
      - alerting
```

## 📈 Performance Optimization

### Redis Optimization
```bash
# Monitor Redis performance
docker-compose exec redis redis-cli INFO stats
docker-compose exec redis redis-cli SLOWLOG GET 10

# Optimize memory usage
docker-compose exec redis redis-cli CONFIG SET maxmemory-policy allkeys-lru
```

### Application Optimization
```bash
# Profile Python performance
docker-compose exec cupcake-app python -m cProfile scripts/websocket_client.py

# Monitor resource usage
docker-compose exec cupcake-app top
docker-compose exec cupcake-app iostat -x 1
```

## 🚀 Production Deployment

### Security Considerations
1. **Use secrets management** for credentials
2. **Enable Redis authentication** in production
3. **Use TLS/SSL** for external connections
4. **Implement proper firewall rules**
5. **Regular security updates**

### Scaling
```bash
# Scale specific services
docker-compose up -d --scale cupcake-websocket=3

# Use Docker Swarm for cluster deployment
docker swarm init
docker stack deploy -c docker-compose.yml cupcake-stack
```

### Backup and Recovery
```bash
# Backup Redis data
docker-compose exec redis redis-cli BGSAVE
docker cp $(docker-compose ps -q redis):/data/dump.rdb ./backup/

# Backup logs
tar -czf logs_backup_$(date +%Y%m%d).tar.gz logs/
```

## 📞 Support

### Useful Commands Reference
```bash
# Complete cleanup
docker-compose down -v --rmi all

# Reset everything
docker-compose down -v
docker system prune -a
docker volume prune

# View resource usage
docker-compose top
docker system df

# Backup entire setup
docker-compose config > current_setup.yml
```

### Log Locations
- **Application logs**: `./logs/` (mounted from container)
- **Redis logs**: `docker-compose logs redis`
- **Docker logs**: `docker-compose logs [service_name]`

This Docker setup provides a robust, scalable environment for running the Tick-Tick Stock Monitoring Program with proper isolation, persistence, and monitoring capabilities.