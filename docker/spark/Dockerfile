FROM apache/spark:3.5.1

USER root

# MySQL JDBC driver
ADD https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.33/mysql-connector-j-8.0.33.jar \
    /opt/spark/jars/mysql-connector-j-8.0.33.jar

# Hadoop S3A + AWS SDK — cần cho MinIO (S3-compatible)
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    /opt/spark/jars/hadoop-aws-3.3.4.jar
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
    /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar

RUN chmod 644 /opt/spark/jars/mysql-connector-j-8.0.33.jar \
              /opt/spark/jars/hadoop-aws-3.3.4.jar \
              /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar

USER spark
