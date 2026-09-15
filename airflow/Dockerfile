FROM apache/airflow:3.3.1-python3.13

USER root

# Для peer-аутентификации через смонтированный Unix socket эти значения должны
# совпадать с UID/GID пользователя postgres на хосте PostgreSQL.
ARG POSTGRES_UID=999
ARG POSTGRES_GID=999

RUN apt-get update \
    && apt-get install -y --no-install-recommends postgresql-client sudo \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid "${POSTGRES_GID}" postgres \
    && useradd --uid "${POSTGRES_UID}" --gid postgres \
         --no-create-home --shell /usr/sbin/nologin postgres

COPY airflow/airflow-postgres.sudoers /etc/sudoers.d/airflow-postgres
RUN chmod 0440 /etc/sudoers.d/airflow-postgres \
    && visudo -cf /etc/sudoers.d/airflow-postgres

USER airflow
