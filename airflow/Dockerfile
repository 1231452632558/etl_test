FROM apache/airflow:3.3.1-python3.13

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends openssh-client \
    && rm -rf /var/lib/apt/lists/*

USER airflow
