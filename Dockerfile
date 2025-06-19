# Multi-stage Dockerfile for Tick-Tick Stock Monitoring Program

# Stage 1: Build stage with compilation tools
FROM python:3.10-slim as builder

# Install system dependencies for building
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    make \
    wget \
    unzip \
    protobuf-compiler \
    libprotobuf-dev \
    pkg-config \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install TA-Lib from source (required for technical analysis)
RUN wget http://prdownloads.sourceforge.net/ta-lib/ta-lib-0.4.0-src.tar.gz && \
    tar -xzf ta-lib-0.4.0-src.tar.gz && \
    cd ta-lib/ && \
    ./configure --prefix=/usr/local && \
    make && \
    make install && \
    cd .. && \
    rm -rf ta-lib* && \
    ldconfig

# Set Python environment
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# Create app directory
WORKDIR /app

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Stage 2: Runtime stage
FROM python:3.10-slim as runtime

# Install runtime system dependencies
RUN apt-get update && apt-get install -y \
    protobuf-compiler \
    redis-tools \
    bc \
    curl \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Copy TA-Lib from builder stage
COPY --from=builder /usr/local/lib/libta_lib.* /usr/local/lib/
COPY --from=builder /usr/local/include/ta_lib/ /usr/local/include/ta_lib/
RUN ldconfig

# Copy Python packages from builder
COPY --from=builder /usr/local/lib/python3.10/site-packages /usr/local/lib/python3.10/site-packages

# Set environment variables
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1
ENV TZ=Asia/Kolkata
ENV CUPCAKE_LOG_DIR=/app/logs
ENV CUPCAKE_CREDENTIALS_PATH=/app/authentication/credentials.json
ENV CUPCAKE_SYMBOLS_PATH=/app/scripts/symbols.json
ENV REDIS_HOST=redis
ENV REDIS_PORT=6379
ENV REDIS_DB=0

# Create application user for security
RUN useradd -m -u 1000 trader && \
    mkdir -p /app/logs /app/authentication /app/scripts /app/web /app/bash_scripts && \
    chown -R trader:trader /app

# Switch to non-root user
USER trader

# Set working directory
WORKDIR /app

# Copy application files
COPY --chown=trader:trader authentication/ ./authentication/
COPY --chown=trader:trader scripts/ ./scripts/
COPY --chown=trader:trader web/ ./web/
COPY --chown=trader:trader bash_scripts/ ./bash_scripts/

# Create logs directory with proper permissions
RUN mkdir -p logs && chmod 755 logs

# Compile Protocol Buffers
RUN cd scripts && \
    protoc --python_out=. MarketDataFeed.proto && \
    chmod +x /app/bash_scripts/*.sh

# Health check script
COPY --chown=trader:trader docker-healthcheck.sh /app/
RUN chmod +x /app/docker-healthcheck.sh

# Expose ports (if needed for web interface)
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD /app/docker-healthcheck.sh

# Default command
CMD ["python", "scripts/Data_Fetcher_Runner.py", "--mode", "both"]