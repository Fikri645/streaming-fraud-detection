# Cloud Run image: serving layer only (FastAPI /score + live dashboard).
# Online features are read from Upstash Redis via the REDIS_URL env var.
FROM python:3.11-slim

WORKDIR /app

# LightGBM needs the OpenMP runtime, which python:3.11-slim does not ship —
# without libgomp1 the container dies at `import lightgbm`
# (OSError: libgomp.so.1: cannot open shared object file).
RUN apt-get update && apt-get install -y --no-install-recommends libgomp1 \
    && rm -rf /var/lib/apt/lists/*

# lean deps — the API needs neither the broker client nor the stream processor
RUN pip install --no-cache-dir \
    fastapi "uvicorn[standard]" sse-starlette pydantic "redis>=5.0" \
    "lightgbm>=4.3" "scikit-learn>=1.4,<1.6" joblib "pandas>=2.1" "numpy<2.0"

COPY src/ src/
COPY api/ api/
COPY models/ models/

ENV PORT=8080
CMD exec uvicorn api.main:app --host 0.0.0.0 --port ${PORT}
