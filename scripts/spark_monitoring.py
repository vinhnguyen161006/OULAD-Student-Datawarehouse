"""
spark_monitoring.py
===================
Script kiểm tra chất lượng dữ liệu của hệ thống.
Ghi log vào bảng monitoring_log.
"""

import os
import logging
from datetime import datetime
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
import mysql.connector

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

MYSQL_HOST = os.getenv("MYSQL_HOST", "mysql")
MYSQL_PORT = os.getenv("MYSQL_PORT", "3306")
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "rootpassword")
SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")

JDBC_URL = f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/student_dwh?useSSL=false&allowPublicKeyRetrieval=true"
JDBC_PROPS = {
    "user": MYSQL_USER,
    "password": MYSQL_PASSWORD,
    "driver": "com.mysql.cj.jdbc.Driver",
}

def write_log_to_mysql(check_name, status, detail):
    """Ghi trực tiếp một dòng log vào MySQL bằng mysql-connector."""
    try:
        conn = mysql.connector.connect(
            host=MYSQL_HOST, port=int(MYSQL_PORT),
            user=MYSQL_USER, password=MYSQL_PASSWORD,
            database="student_dwh"
        )
        cursor = conn.cursor()
        sql = "INSERT INTO monitoring_log (check_name, status, detail) VALUES (%s, %s, %s)"
        cursor.execute(sql, (check_name, status, detail))
        conn.commit()
        cursor.close()
        conn.close()
        log.info(f"[LOGGED] {check_name} - {status} - {detail}")
    except Exception as e:
        log.error(f"Lỗi khi ghi log: {e}")

def main():
    spark = SparkSession.builder \
        .appName("data_quality_monitoring") \
        .master(SPARK_MASTER) \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")

    try:
        log.info("Bắt đầu kiểm tra Data Quality trên bảng Fact_Performance...")
        
        # 1. Đọc bảng Fact
        fact_df = spark.read.jdbc(url=JDBC_URL, table="Fact_Performance", properties=JDBC_PROPS)
        
        # 2. Check 1: Row Count (Phải lớn hơn 0)
        total_rows = fact_df.count()
        if total_rows > 0:
            write_log_to_mysql("Fact Row Count", "OK", f"Bảng Fact có {total_rows} dòng.")
        else:
            write_log_to_mysql("Fact Row Count", "FAIL", "Bảng Fact trống (0 dòng)!")

        # 3. Check 2: Tỷ lệ NULL của điểm số (avg_score)
        null_score_count = fact_df.filter(F.col("avg_score").isNull()).count()
        null_rate = (null_score_count / total_rows) * 100 if total_rows > 0 else 0
        
        if null_rate < 5.0: # Giả sử cho phép tối đa 5% sinh viên không có điểm
            write_log_to_mysql("Null Score Rate", "OK", f"Tỷ lệ NULL của avg_score là {null_rate:.2f}% ({null_score_count} dòng)")
        elif null_rate < 20.0:
            write_log_to_mysql("Null Score Rate", "WARN", f"Tỷ lệ NULL của avg_score hơi cao: {null_rate:.2f}%")
        else:
            write_log_to_mysql("Null Score Rate", "FAIL", f"Tỷ lệ NULL của avg_score quá cao: {null_rate:.2f}%")

        log.info("Hoàn tất Monitoring.")

    finally:
        spark.stop()

if __name__ == "__main__":
    main()