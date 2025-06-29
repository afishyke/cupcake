# Docker Setup for Jith Trading System

This guide explains how to run the trading system using Docker with Redis support.

## Prerequisites

- Docker and Docker Compose installed
- `credentials.json` and `symbols.json` files in the project root

## Quick Start

### Option 1: Using Docker Compose (Recommended)

1. **Build and run the complete system:**
   ```bash
   docker-compose up --build
   ```

2. **Run in background:**
   ```bash
   docker-compose up -d --build
   ```

3. **View logs:**
   ```bash
   docker-compose logs -f app
   docker-compose logs -f redis
   ```

4. **Stop the system:**
   ```bash
   docker-compose down
   ```

### Option 2: Using Docker only

1. **Start Redis container:**
   ```bash
   docker run -d --name redis -p 6379:6379 redis:7-alpine
   ```

2. **Build the application:**
   ```bash
   docker build -t jith-trading .
   ```

3. **Run the application:**
   ```bash
   docker run -p 5000:5000 --link redis:redis \
     -e REDIS_HOST=redis \
     -v $(pwd)/credentials.json:/app/credentials.json:ro \
     -v $(pwd)/symbols.json:/app/symbols.json:ro \
     jith-trading
   ```

## Configuration

### Environment Variables

The system uses the following environment variables for Redis configuration:

- `REDIS_HOST`: Redis hostname (default: localhost)
- `REDIS_PORT`: Redis port (default: 6379)

### Docker Compose Configuration

The `docker-compose.yml` includes:

- **Redis service**: Persistent data storage with health checks
- **Application service**: Trading system with environment variables
- **Volume mounts**: For credentials and configuration files
- **Health checks**: Ensures Redis is ready before starting the app

## Services and Ports

- **Web Dashboard**: http://localhost:5000
- **Historical Viewer**: http://localhost:5000/historical
- **Redis**: localhost:6379 (internal communication)

## Troubleshooting

### Redis Connection Issues

1. **Check Redis service status:**
   ```bash
   docker-compose ps
   ```

2. **Test Redis connection:**
   ```bash
   docker-compose exec redis redis-cli ping
   ```

3. **Check application logs:**
   ```bash
   docker-compose logs app | grep -i redis
   ```

### Application Issues

1. **View all logs:**
   ```bash
   docker-compose logs -f
   ```

2. **Restart services:**
   ```bash
   docker-compose restart
   ```

3. **Rebuild if needed:**
   ```bash
   docker-compose down
   docker-compose up --build
   ```

### File Permission Issues

If you encounter file permission issues:

```bash
# Fix ownership
sudo chown -R $USER:$USER .

# Ensure files are readable
chmod 644 credentials.json symbols.json
```

## Data Persistence

- Redis data is persisted in a Docker volume named `redis_data`
- Data survives container restarts
- To reset all data: `docker-compose down -v`

## Development Mode

For development with live code changes:

```bash
# Mount source code directory
docker-compose -f docker-compose.yml -f docker-compose.dev.yml up
```

Create `docker-compose.dev.yml`:
```yaml
version: '3.8'
services:
  app:
    volumes:
      - .:/app
    environment:
      - FLASK_ENV=development
```

## Production Deployment

For production:

1. **Use production configuration:**
   ```yaml
   environment:
     - REDIS_HOST=your-redis-host
     - REDIS_PORT=6379
   ```

2. **Security considerations:**
   - Use Redis authentication
   - Set up proper network security
   - Use secrets management for credentials

## System Components

The Docker setup includes all system components:

- **Enhanced Live Fetcher**: Real-time market data
- **Signal Generator**: Trading signal analysis
- **Portfolio Tracker**: Portfolio monitoring
- **Historical Data Fetcher**: Historical data management
- **Technical Indicators**: Market analysis
- **Redis Database**: Data storage and caching

## Monitoring

Check system health:

```bash
# Application health
curl http://localhost:5000/api/system_status

# Redis health
docker-compose exec redis redis-cli info server
```

## Logs and Debugging

View specific component logs:

```bash
# All logs
docker-compose logs -f

# Application only
docker-compose logs -f app

# Redis only  
docker-compose logs -f redis

# Search for errors
docker-compose logs app | grep -i error
```