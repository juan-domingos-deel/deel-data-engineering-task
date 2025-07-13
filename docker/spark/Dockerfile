FROM bitnami/spark:latest

USER root
RUN apt-get update && apt-get install -y curl

WORKDIR /app

# Copy your Spark script
COPY etl/spark_job.py /app/spark-job.py

# Install Python dependencies
RUN pip install psycopg2-binary

# Download PostgreSQL JDBC driver
RUN curl -L -o postgresql-42.7.3.jar https://jdbc.postgresql.org/download/postgresql-42.7.3.jar

RUN mkdir -p /app/artifacts && chown -R 1001:1001 /app/artifacts

USER 1001