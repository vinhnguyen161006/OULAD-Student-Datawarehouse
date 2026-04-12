"""
upload_to_minio.py
==================
Upload 7 file CSV OULAD từ data/raw/ lên MinIO bucket oulad-bronze.
Chạy một lần sau khi download data: make upload-data

Biến môi trường:
    MINIO_ENDPOINT   — mặc định http://localhost:9000 (local) hoặc http://minio:9000 (Docker)
    MINIO_ACCESS_KEY — mặc định minioadmin
    MINIO_SECRET_KEY — mặc định minioadmin
    MINIO_BUCKET     — mặc định oulad-bronze
    DATA_DIR         — mặc định data/raw
"""

import os
import sys
import logging

from minio import Minio
from minio.error import S3Error

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

ENDPOINT   = os.getenv("MINIO_ENDPOINT", "http://localhost:9000").replace("http://", "").replace("https://", "")
ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
BUCKET     = os.getenv("MINIO_BUCKET", "oulad-bronze")
DATA_DIR   = os.getenv("DATA_DIR", "data/raw")

CSV_FILES = [
    "courses.csv",
    "assessments.csv",
    "vle.csv",
    "studentInfo.csv",
    "studentRegistration.csv",
    "studentAssessment.csv",
    "studentVle.csv",
]


def main():
    log.info(f"MinIO endpoint : {ENDPOINT}")
    log.info(f"Bucket         : {BUCKET}")
    log.info(f"Data dir       : {DATA_DIR}")

    client = Minio(ENDPOINT, access_key=ACCESS_KEY, secret_key=SECRET_KEY, secure=False)

    if not client.bucket_exists(BUCKET):
        client.make_bucket(BUCKET)
        log.info(f"[CREATE] bucket '{BUCKET}' created")
    else:
        log.info(f"[OK    ] bucket '{BUCKET}' already exists")

    errors = []
    for csv_file in CSV_FILES:
        local_path = os.path.join(DATA_DIR, csv_file)
        if not os.path.exists(local_path):
            log.error(f"[SKIP  ] {local_path} not found — chạy 'make setup-data' trước")
            errors.append(csv_file)
            continue
        try:
            client.fput_object(BUCKET, csv_file, local_path)
            size_mb = os.path.getsize(local_path) / 1024 / 1024
            log.info(f"[UP    ] {csv_file} → s3a://{BUCKET}/{csv_file}  ({size_mb:.1f} MB)")
        except S3Error as e:
            log.error(f"[ERROR ] {csv_file}: {e}")
            errors.append(csv_file)

    if errors:
        log.error(f"Upload thất bại: {errors}")
        sys.exit(1)

    log.info("=== Upload hoàn tất ===")


if __name__ == "__main__":
    main()
