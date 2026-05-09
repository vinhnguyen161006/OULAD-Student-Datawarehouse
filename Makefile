.PHONY: up down restart build logs airflow-logs minio-shell mysql-shell dbt-run dbt-test dbt-docs clean help setup-data upload-data monitoring all wait-healthy trigger-pipeline metabase-setup metabase-export

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

## dbt-docs: Generate + serve dbt docs ở port 8088
dbt-docs:
	docker compose exec airflow-scheduler \
		dbt docs generate --project-dir /opt/dbt_student --profiles-dir /opt/dbt_student
	docker compose exec airflow-scheduler \
		dbt docs serve --project-dir /opt/dbt_student --port 8088 --host 0.0.0.0

## wait-healthy: Đợi tới khi airflow-scheduler healthy (~60s)
wait-healthy:
	@echo "Đợi Airflow scheduler healthy..."
	@for i in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20; do \
		status=$$(docker compose ps airflow-scheduler --format '{{.Status}}' 2>/dev/null); \
		if echo "$$status" | grep -q "healthy"; then echo "Ready."; exit 0; fi; \
		sleep 5; \
	done; \
	echo "Timeout chờ scheduler healthy"; exit 1

## trigger-pipeline: Bật + trigger DAG bronze_ingest qua Airflow CLI
trigger-pipeline:
	docker compose exec airflow-scheduler airflow dags unpause bronze_ingest
	docker compose exec airflow-scheduler airflow dags unpause silver_processing
	docker compose exec airflow-scheduler airflow dags unpause dwh_load
	docker compose exec airflow-scheduler airflow dags unpause gold_dbt_run
	docker compose exec airflow-scheduler airflow dags unpause monitoring
	docker compose exec airflow-scheduler airflow dags trigger bronze_ingest

## metabase-setup: Idempotent setup admin + 2 DB connections + import 4 dashboards (29 cards) từ metabase/dashboards/*.json
metabase-setup:
	@PYTHONIOENCODING=utf-8 python -X utf8 scripts/metabase_setup.py

## metabase-export: Export 4 dashboards hiện tại từ Metabase ra metabase/dashboards/*.json (dùng khi sửa dashboard trên UI)
metabase-export:
	@PYTHONIOENCODING=utf-8 python -X utf8 scripts/metabase_export.py

## all: Bootstrap end-to-end — up + upload + trigger pipeline + setup Metabase dashboards (cần data/raw/ đã có CSV)
all: up wait-healthy upload-data trigger-pipeline metabase-setup
	@echo ""
	@echo "============================================================"
	@echo "Pipeline đã trigger. Theo dõi tại http://localhost:8080"
	@echo "Metabase dashboards sẵn sàng tại http://localhost:3000"
	@echo "  Login: admin@oulad.local / oulad12345"
	@echo "============================================================"

## clean: Dừng stack VÀ xoá toàn bộ volume (cẩn thận!)
clean:
	docker compose down -v

## help: Hiển thị danh sách lệnh
help:
	@grep -E '^## ' Makefile | sed 's/## //'
