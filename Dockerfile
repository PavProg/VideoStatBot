FROM python:3.11-slim as builder

# Установка системных зависимостей
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Установка Poetry
RUN pip install poetry==1.7.0

WORKDIR /app
COPY pyproject.toml poetry.lock* ./

RUN poetry config virtualenvs.create false \
    && poetry install --no-dev --no-interaction --no-ansi

FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

RUN useradd -m -u 1000 appuser
WORKDIR /app

COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

COPY --chown=appuser:appuser . .

ENV PYTHONPATH=/app/src
ENV PYTHONUNBUFFERED=1
ENV PATH="/home/appuser/.local/bin:${PATH}"

USER appuser

RUN mkdir -p /app/logs

CMD ["python", "src/main.py"]