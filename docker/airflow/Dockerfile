FROM apache/airflow:2.9.2-python3.11

USER root

# Cài OpenJDK — cần cho PySpark
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        openjdk-17-jdk-headless \
        default-libmysqlclient-dev \
        pkg-config \
        build-essential \
        curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# MySQL JDBC jar — cần cho Spark client mode (driver chạy trên Airflow container)
RUN mkdir -p /opt/airflow/jars \
    && curl -fL \
        "https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.33/mysql-connector-j-8.0.33.jar" \
        -o /opt/airflow/jars/mysql-connector-j-8.0.33.jar

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

COPY requirements.txt /tmp/airflow-requirements.txt
RUN pip install --no-cache-dir -r /tmp/airflow-requirements.txt
