FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

RUN pip install --upgrade pip && pip install \
    "soda-core-postgres>=3.3.0" \
    "apscheduler>=3.10.0" \
    "prometheus-client>=0.20.0"

COPY runner.py /app/runner.py
COPY checks.yml /etc/soda/checks.yml
COPY configuration.yml /etc/soda/configuration.yml

WORKDIR /app
EXPOSE 8002

CMD ["python", "runner.py"]
