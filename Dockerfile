# CellMapper Scraper - Dockerfile
FROM python:3.12-slim

# Install system dependencies for Playwright
RUN apt-get update && apt-get install -y \
    bash \
    redis-tools \
    wget \
    gnupg \
    libnss3 \
    libnspr4 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdrm2 \
    libxkbcommon0 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxrandr2 \
    libgbm1 \
    libasound2 \
    libpango-1.0-0 \
    libcairo2 \
    libatspi2.0-0 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install Playwright browsers
RUN playwright install chromium

# Copy application code
COPY . .

# Create directories for data persistence
RUN mkdir -p /app/data/towers /app/logs

# Ensure entrypoint script is executable
RUN chmod +x /app/scripts/entrypoint_wait_cookie.sh

# Default command
CMD ["python", "main.py", "--auto-refresh", "--proxies", "--format", "sqlite"]




