FROM bitnami/spark:3

USER root
RUN apt-get update && apt-get install -y curl

WORKDIR /app

RUN /opt/bitnami/python/bin/pip install pyspark==3.4.4 delta-spark==2.4.0
RUN #pip install delta-spark

# Copy your Spark script
COPY etl/spark_cdc_job.py /app/spark_cdc_job.py

# Install Python dependencies
RUN #pip install psycopg2-binary

# Download PostgreSQL JDBC driver
RUN curl -L -o postgresql-42.7.3.jar https://jdbc.postgresql.org/download/postgresql-42.7.3.jar

RUN mkdir -p /app/artifacts && chown -R 1001:1001 /app/artifacts

USER 1001