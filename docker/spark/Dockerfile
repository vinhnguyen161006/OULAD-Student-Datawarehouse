FROM bitnamilegacy/spark:3.5.1

USER root

# Dùng lệnh ADD tích hợp sẵn của Docker để tải file
ADD https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.33/mysql-connector-j-8.0.33.jar /opt/bitnami/spark/jars/mysql-connector-j-8.0.33.jar

# Chỉnh lại quyền sở hữu file cho user Spark (mặc định là 1001)
RUN chmod 644 /opt/bitnami/spark/jars/mysql-connector-j-8.0.33.jar

USER 1001