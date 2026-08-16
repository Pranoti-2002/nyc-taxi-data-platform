FROM apache/airflow:3.0.0-python3.11

USER root

# Install Java 17 for Spark/PySpark
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        openjdk-17-jdk-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

COPY docker/airflow/requirements.txt /tmp/requirements.txt

USER airflow

RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.0.0/constraints-3.11.txt" \
    -r /tmp/requirements.txt

USER root

COPY docker/airflow/entrypoint.sh /entrypoint.sh

RUN chmod +x /entrypoint.sh

USER airflow

ENTRYPOINT ["/entrypoint.sh"]