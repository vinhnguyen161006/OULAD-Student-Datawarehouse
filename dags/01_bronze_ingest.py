"""
DAG 01 — bronze_ingest
=======================
Kiểm tra 7 file CSV OULAD đã có trong MinIO oulad-bronze chưa.
Nếu đủ → publish Dataset để trigger DAG 02 (silver_processing).

Input  : MinIO bucket oulad-bronze (upload bằng: make upload-data)
Output : Airflow Dataset s3://oulad-bronze
         → trigger tự động 02_silver_processing

Thành viên phụ trách: Tú
"""

from datetime import datetime

from airflow.datasets import Dataset
from airflow.decorators import dag, task

BRONZE_DATASET = Dataset("s3://oulad-bronze")

EXPECTED_FILES = [
    "courses.csv",
    "assessments.csv",
    "vle.csv",
    "studentInfo.csv",
    "studentRegistration.csv",
    "studentAssessment.csv",
    "studentVle.csv",
]


@dag(
    dag_id="bronze_ingest",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["bronze", "minio", "oulad"],
    doc_md="""
## DAG 01 — Bronze Ingest

**Mục đích**: Xác nhận 7 file CSV OULAD đã có trong MinIO `oulad-bronze`.

**Chuẩn bị trước khi trigger**:
```bash
make setup-data   # tải CSV về data/raw/
make upload-data  # upload lên MinIO oulad-bronze
```

**Output dataset**: `s3://oulad-bronze`
→ Tự động trigger `silver_processing` (DAG 02) khi hoàn thành.
    """,
)
def bronze_ingest_dag():

    @task(outlets=[BRONZE_DATASET])
    def check_bronze_files():
        import os
        from minio import Minio

        endpoint   = os.getenv("MINIO_ENDPOINT", "http://minio:9000").replace("http://", "")
        access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
        secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")
        bucket     = os.getenv("MINIO_BUCKET", "oulad-bronze")

        client = Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=False)

        if not client.bucket_exists(bucket):
            raise FileNotFoundError(
                f"Bucket '{bucket}' không tồn tại. Chạy 'make upload-data' trước."
            )

        missing = []
        for f in EXPECTED_FILES:
            try:
                client.stat_object(bucket, f)
            except Exception:
                missing.append(f)

        if missing:
            raise FileNotFoundError(
                f"Thiếu {len(missing)} file trong '{bucket}': {missing}\n"
                "Chạy 'make upload-data' để upload."
            )

        print(f"[OK] Tất cả {len(EXPECTED_FILES)} CSV files có trong s3://{bucket}/")

    check_bronze_files()


bronze_ingest_dag()
