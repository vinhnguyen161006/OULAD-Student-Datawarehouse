.PHONY: up down restart build logs airflow-logs minio-shell mysql-shell dbt-run dbt-test clean help setup-data upload-data monitoring

## setup-data: Tải dữ liệu OULAD về data/raw/ (chạy lần đầu sau khi clone)
setup-data:
	powershell -ExecutionPolicy Bypass -File download_data.ps1

## upload-data: Upload 7 CSV từ data/raw/ lên MinIO oulad-bronze bucket
upload-data:
	docker compose exec airflow-scheduler \
		python /opt/airflow/scripts/upload_to_minio.py

## up: Khởi động toàn bộ stack (detached)
up:
	docker compose --env-file .env up -d

## down: Dừng stack, giữ nguyên volume
down:
	docker compose down

## restart: Restart tất cả services
restart:
	docker compose restart

## build: Build lại các custom image (airflow, spark)
build:
	docker compose --env-file .env build --no-cache

## logs: Xem log tất cả services (follow)
logs:
	docker compose logs -f

## airflow-logs: Xem log Airflow webserver + scheduler
airflow-logs:
	docker compose logs -f airflow-webserver airflow-scheduler

## minio-shell: Mở MinIO console URL
minio-shell:
	@echo "MinIO Console: http://localhost:9001  (minioadmin / minioadmin)"

## mysql-shell: Mở MySQL shell (student_dwh)
mysql-shell:
	docker compose exec mysql mysql -u root -prootpassword student_dwh

## monitoring: Xem kết quả health checks gần nhất
monitoring:
	docker compose exec mysql mysql -u root -prootpassword student_dwh \
		-e "SELECT check_name, status, detail, checked_at FROM monitoring_log ORDER BY checked_at DESC LIMIT 20;"

## dbt-run: Chạy dbt build bên trong container scheduler
dbt-run:
	docker compose exec airflow-scheduler \
		dbt build --project-dir /opt/dbt_student --profiles-dir /opt/dbt_student

## dbt-test: Chỉ chạy dbt test
dbt-test:
	docker compose exec airflow-scheduler \
		dbt test --project-dir /opt/dbt_student --profiles-dir /opt/dbt_student

## clean: Dừng stack VÀ xoá toàn bộ volume (cẩn thận!)
clean:
	docker compose down -v

## help: Hiển thị danh sách lệnh
help:
	@grep -E '^## ' Makefile | sed 's/## //'
