# syntax=docker/dockerfile:1

FROM python:3.12-slim AS builder

WORKDIR /build

COPY pyproject.toml README.md ./
COPY src/ src/

RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir build \
    && python -m build --wheel --outdir /build/dist

FROM python:3.12-slim

RUN addgroup --system app && adduser --system --ingroup app app

WORKDIR /app

COPY --from=builder /build/dist/*.whl /tmp/
RUN pip install --no-cache-dir /tmp/*.whl && rm /tmp/*.whl

RUN mkdir -p /app/data && chown app:app /app/data

USER app

CMD ["python", "-m", "schulmanager_discord_bot"]
