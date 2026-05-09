"""
DAG 05 — monitoring
====================
Chạy Spark script để kiểm tra chất lượng dữ liệu (Data Quality) 
và ghi kết quả vào bảng monitoring_log trong MySQL.

Input  : Airflow Dataset mysql://student_data_mart/marts
         (trigger tự động từ gold_dbt_run — DAG 04)
Output : Bảng monitoring_log trong DB student_dwh
"""

from datetime import datetime
from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

MARTS_DATASET = Dataset("mysql://student_data_mart/marts")

SPARK_APP = "/opt/airflow/scripts/spark_monitoring.py"

JARS = "/opt/airflow/jars/mysql-connector-j-8.0.33.jar"

_SPARK_CONF = {
    "spark.driver.memory": "1g",
    "spark.executor.memory": "1g",
}

_ENV_VARS = {
    "MYSQL_HOST": "mysql",
    "MYSQL_PORT": "3306",
    "MYSQL_USER": "root",
    "MYSQL_PASSWORD": "rootpassword",
    "SPARK_MASTER": "spark://spark-master:7077",
}

@dag(
    dag_id="monitoring",
    schedule=[MARTS_DATASET], # Trigger khi DAG 04 hoàn thành
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["monitoring", "pyspark", "dq", "oulad"],
    doc_md="""
## DAG 05 — Data Quality Monitoring

**Mục đích**: Chạy các bài test chất lượng dữ liệu (Row count, Null rate) trên bảng Fact và ghi log vào bảng `monitoring_log`.
    """,
)
def monitoring_dag():

    run_dq_checks = SparkSubmitOperator(
        task_id="spark_dq_checks",
        conn_id="spark_default",
        application=SPARK_APP,
        name="monitoring_job",
        deploy_mode="client",
        jars=JARS,
        conf=_SPARK_CONF,
        env_vars=_ENV_VARS,
        verbose=True,
    )

    run_dq_checks

monitoring_dag()