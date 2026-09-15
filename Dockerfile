# Multi-stage Dockerfile for Distributed Search Engine (DSE)
# Phase 26: Containerization

# -----------------------------------------------------------------------------
# Stage 1: Build stage
# -----------------------------------------------------------------------------
FROM debian:bookworm-slim AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    cmake \
    git \
    librdkafka-dev \
    ca-certificates \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY . .

# Configure and build DistributedSearchEngine with Kafka support enabled
RUN cmake -B build -S . \
    -DCMAKE_BUILD_TYPE=Release \
    -DENABLE_KAFKA=ON \
    -DBUILD_TESTING=OFF \
 && cmake --build build --target DistributedSearchEngine -j$(nproc)

# -----------------------------------------------------------------------------
# Stage 2: Runtime stage
# -----------------------------------------------------------------------------
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    librdkafka1 \
    librdkafka++1 \
    ca-certificates \
    curl \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy compiled executable from builder stage
COPY --from=builder /src/build/DistributedSearchEngine /usr/local/bin/DistributedSearchEngine

# Default directory for persistent node storage
RUN mkdir -p /app/data

EXPOSE 8080 9000

ENTRYPOINT ["/usr/local/bin/DistributedSearchEngine"]
