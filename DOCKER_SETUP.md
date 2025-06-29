# Docker Setup and Configuration

## Quick Start

1. **Copy environment template:**
   ```bash
   cp .env.example .env
   ```

2. **Configure your environment** (edit `.env` file):
   ```bash
   # Default configuration works for most users
   APP_HOST=0.0.0.0
   APP_PORT=5000
   REDIS_HOST=redis
   REDIS_PORT=6379
   ```

3. **Ensure required files exist:**
   ```bash
   # Your Upstox API credentials
   ls credentials.json
   
   # Trading symbols configuration  
   ls symbols.json
   ```

4. **Run with Docker Compose:**
   ```bash
   docker-compose up -d
   ```

5. **Access the application:**
   - Dashboard: http://localhost:5000
   - Quantitative Analysis: http://localhost:5000 → "🧮 Quant Analysis" tab

## Environment Variables

### Core Application
- `APP_HOST`: Application bind address (default: `0.0.0.0`)
- `APP_PORT`: Application port (default: `5000`)

### Redis Database  
- `REDIS_HOST`: Redis server hostname (default: `redis`)
- `REDIS_PORT`: Redis server port (default: `6379`)

### File Paths
- `CREDENTIALS_PATH`: Path to Upstox credentials JSON (default: `/app/credentials.json`)
- `LOG_DIR`: Directory for application logs (default: `/app/logs`)
- `CONFIG_DIR`: Directory for configuration files (default: `/app/config`)

## Custom Port Configuration

To run on a different port (e.g., 8080):

1. **Update .env file:**
   ```bash
   APP_PORT=8080
   ```

2. **Restart containers:**
   ```bash
   docker-compose down
   docker-compose up -d
   ```

3. **Access on new port:**
   ```
   http://localhost:8080
   ```

## Volume Mounts

The following directories are mounted for data persistence:

- `./credentials.json` → `/app/credentials.json` (read-only)
- `./symbols.json` → `/app/symbols.json` (read-only)  
- `./logs` → `/app/logs` (read-write)
- `./config` → `/app/config` (read-write)

## Local Development

For local development without Docker:

1. **Set environment variables:**
   ```bash
   export APP_HOST=localhost
   export REDIS_HOST=localhost
   export CREDENTIALS_PATH=./credentials.json
   export LOG_DIR=./logs
   ```

2. **Start Redis:**
   ```bash
   redis-server
   ```

3. **Run application:**
   ```bash
   python main.py
   ```

## Troubleshooting

### Port Already in Use
```bash
# Check what's using the port
lsof -i :5000

# Change port in .env file
echo "APP_PORT=8080" >> .env
docker-compose up -d
```

### Redis Connection Issues
```bash
# Check Redis container health
docker-compose ps

# View Redis logs
docker-compose logs redis

# Reset Redis data
docker-compose down -v
docker-compose up -d
```

### File Permission Issues
```bash
# Ensure log directory exists and is writable
mkdir -p logs config
chmod 755 logs config
```