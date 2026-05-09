FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY pyproject.toml /app/pyproject.toml
RUN pip install --upgrade pip && pip install .

COPY consumer /app/consumer

EXPOSE 8001

CMD ["python", "-m", "consumer"]
