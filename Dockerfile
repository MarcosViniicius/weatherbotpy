# ════════════════════════════════════════════════════════════
# WeatherBot v3 — Lightweight Docker Image
# Optimized for VPS deployment
# ════════════════════════════════════════════════════════════

# Stage 1: Builder (install dependencies)
FROM python:3.12-slim AS builder

WORKDIR /build

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements.txt .

# Install Python dependencies
RUN pip install --user --no-cache-dir -r requirements.txt

# Stage 2: Runtime (final image)
FROM python:3.12-slim

# Metadata
LABEL maintainer="WeatherBot"
LABEL description="WeatherBot v3 - Polymarket Trading Bot"
LABEL version="3.0"

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH=/root/.local/bin:$PATH \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Install runtime dependencies only (minimal footprint)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy Python packages from builder
COPY --from=builder /root/.local /root/.local

# Copy application code
COPY . .

# Create data directory
RUN mkdir -p data/markets && chmod 755 data

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -sf http://localhost:8877/ > /dev/null || exit 1

# Expose dashboard port
EXPOSE 8877

# Run the bot
CMD ["python", "main.py"]
