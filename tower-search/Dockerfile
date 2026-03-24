# Stage 1: Build React frontend
FROM node:20-alpine AS frontend
RUN corepack enable && corepack prepare pnpm@9.12.0 --activate
WORKDIR /app/web
COPY web/package.json web/pnpm-lock.yaml ./
RUN pnpm install --frozen-lockfile
COPY web/ .
RUN pnpm build

# Stage 2: Python API
FROM python:3.12-slim
WORKDIR /app/api

COPY api/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY api/ ./
COPY --from=frontend /app/web/dist ./static/

# Copy the pre-built SQLite database (build image locally where towers.db exists)
COPY data/towers.db /app/data/towers.db

ENV TOWERS_DB_PATH=/app/data/towers.db

EXPOSE 8000

# PORT env var is injected by Railway; falls back to 8000 locally
CMD ["sh", "-c", "uvicorn main:app --host 0.0.0.0 --port ${PORT:-8000} --workers 2"]
