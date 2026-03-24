# Stage 1: Build React frontend
FROM node:20-alpine AS frontend
RUN corepack enable && corepack prepare pnpm@9.12.0 --activate
WORKDIR /app/web
COPY web/package.json web/pnpm-lock.yaml ./
RUN pnpm install --frozen-lockfile
COPY web/ .
RUN pnpm build

# Stage 2: Python API + serve static files
FROM python:3.12-slim
WORKDIR /app

COPY api/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY api/ ./api/
COPY --from=frontend /app/web/dist ./api/static/

# Import data (build the SQLite DB into the image)
# The JSONL file is expected at /app/data/towers_geocoded_500k.jsonl at build time
ARG DATA_FILE=data/towers_geocoded_500k.jsonl
COPY ${DATA_FILE} ./data/
RUN python api/import_data.py ./data/towers_geocoded_500k.jsonl ./data/towers.db && \
    rm ./data/towers_geocoded_500k.jsonl

ENV TOWERS_DB_PATH=/app/data/towers.db
EXPOSE 8000

CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "2"]
