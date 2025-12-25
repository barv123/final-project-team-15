# Официальный образ Airflow:
FROM apache/airflow:2.8.1

# Переключаемся на пользователя root для установки системных пакетов
USER root

# Устанавливаем Java (для PySpark) и curl (для скачивания драйвера)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    curl \
    && apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Создаем директорию для JDBC драйвера и скачиваем его
ARG POSTGRES_JDBC_VERSION=42.7.1
RUN mkdir -p /opt/airflow/jars
RUN curl -o /opt/airflow/jars/postgresql-${POSTGRES_JDBC_VERSION}.jar https://jdbc.postgresql.org/download/postgresql-${POSTGRES_JDBC_VERSION}.jar

# Переключаемся обратно на пользователя airflow
USER airflow

# Устанавливаем необходимые Python-библиотеки
RUN pip install --no-cache-dir \
    apache-airflow-providers-postgres \
    pandas \
    pyarrow \
    pyspark