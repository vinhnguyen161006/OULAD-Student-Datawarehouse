"""
DAG 04 — gold_dbt_run
======================
Chạy dbt build để tạo các bảng Data Mart (Gold layer) trong MySQL.

Input  : Airflow Dataset mysql://student_dwh/fact_performance
         (trigger tự động từ dwh_load — DAG 03)
Output : Airflow Dataset mysql://student_data_mart/marts
         (trigger tự động monitoring — DAG 05)
"""

from datetime import datetime
from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

FACT_PERF_DATASET = Dataset("mysql://student_dwh/fact_performance")
MARTS_DATASET     = Dataset("mysql://student_data_mart/marts")

# Thư mục chứa dbt project đã được mount vào container
DBT_DIR = "/opt/dbt_student"

@dag(
    dag_id="gold_dbt_run",
    schedule=[FACT_PERF_DATASET], # Tự động chạy khi DAG 03 xong
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["gold", "dbt", "mysql", "mart", "oulad"],
    doc_md="""
## DAG 04 — Gold Layer (dbt Marts)

**Mục đích**: Sử dụng dbt để transform dữ liệu từ `student_dwh` sang các bảng tổng hợp (Marts) tại `student_data_mart`. Đồng thời chạy các data tests.

**Input dataset**: `mysql://student_dwh/fact_performance`
**Output dataset**: `mysql://student_data_mart/marts`
    """,
)
def gold_dbt_run_dag():

    # Chạy lệnh dbt build (bao gồm dbt run và dbt test)
    dbt_build = BashOperator(
        task_id="dbt_build_marts",
        bash_command=f"dbt build --project-dir {DBT_DIR} --profiles-dir {DBT_DIR}",
    )

    @task(outlets=[MARTS_DATASET])
    def publish_mart_dataset():
        """Publish dataset để báo hiệu Mart đã load xong → trigger DAG 05."""
        print("Đã hoàn thành chạy dbt build!")

    dbt_build >> publish_mart_dataset()

gold_dbt_run_dag()