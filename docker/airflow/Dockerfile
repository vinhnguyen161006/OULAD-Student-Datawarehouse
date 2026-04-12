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

# JDBC + S3A jars — cần cho Spark client mode (driver chạy trên Airflow container)
RUN mkdir -p /opt/airflow/jars \
    && curl -fL \
        "https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.33/mysql-connector-j-8.0.33.jar" \
        -o /opt/airflow/jars/mysql-connector-j-8.0.33.jar \
    && curl -fL \
        "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar" \
        -o /opt/airflow/jars/hadoop-aws-3.3.4.jar \
    && curl -fL \
        "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar" \
        -o /opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

COPY requirements.txt /tmp/airflow-requirements.txt
RUN pip install --no-cache-dir -r /tmp/airflow-requirements.txt
