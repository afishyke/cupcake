# Use an official Python runtime as a parent image
FROM python:3.10-buster

# Install TA-Lib dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Download, compile, and install TA-Lib
RUN wget http://prdownloads.sourceforge.net/ta-lib/ta-lib-0.4.0-src.tar.gz && \
    tar -xzf ta-lib-0.4.0-src.tar.gz && \
    cd ta-lib/ && \
    ./configure --prefix=/usr && \
    make && \
    make install && \
    cd .. && \
    rm -rf ta-lib ta-lib-0.4.0-src.tar.gz

# Create a symbolic link to the TA-Lib library
RUN ln -s /usr/lib/libta_lib.so.0 /usr/lib/libta-lib.so

# Set the working directory in the container
WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --no-cache-dir TA-Lib

# Copy the rest of the application code
COPY . .

# Generate Protobuf files
RUN python -m grpc_tools.protoc -I=. --python_out=. --grpc_python_out=. MarketDataFeedV3.proto

# Expose port
EXPOSE 5000

# Command to run the application
CMD ["python", "main.py"]