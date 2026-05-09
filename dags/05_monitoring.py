# DAG 05 — monitoring: health checks DWH + Mart → student_dwh.monitoring_log

from datetime import datetime
from textwrap import dedent

from airflow.datasets import Dataset
from airflow.decorators import dag
from airflow.operators.bash import BashOperator

MART_DATASET = Dataset("mysql://student_data_mart")

# Chạy local mode trong scheduler container — job rất nhẹ và tránh được
# Python version mismatch giữa driver (3.11) và spark-worker.
SPARK_SUBMIT_CMD = dedent("""
    spark-submit \
        --master "local[*]" \
        --deploy-mode client \
        --jars /opt/airflow/jars/mysql-connector-j-8.0.33.jar \
        --conf spark.driver.memory=1g \
        --conf spark.sql.shuffle.partitions=4 \
        /opt/airflow/scripts/spark_monitoring.py
""").strip()

_ENV = {
    "MYSQL_HOST":     "mysql",
    "MYSQL_PORT":     "3306",
    "MYSQL_USER":     "root",
    "MYSQL_PASSWORD": "rootpassword",
}


@dag(
    dag_id="monitoring",
    schedule=[MART_DATASET],
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["monitoring", "pyspark", "mysql", "oulad"],
    doc_md="""
**Input**: `mysql://student_data_mart` → trigger bởi `gold_dbt_run`
**Output**: `student_dwh.monitoring_log` (append rows)
**Job**: `scripts/spark_monitoring.py` (local Spark)

**Checks**:
- Row counts cho Dim_*, Fact_Performance, mart_*
- NULL rate trên Fact_Performance (foreign keys + score)
- Accepted values cho final_result / risk_group

DAG fail nếu có >= 1 check `FAIL` hoặc `ERROR` (WARN không fail).
Xem kết quả gần nhất bằng `make monitoring`.
    """,
)
def monitoring_dag():

    BashOperator(
        task_id="spark_monitoring",
        bash_command=SPARK_SUBMIT_CMD,
        env=_ENV,
        append_env=True,
    )


monitoring_dag()
