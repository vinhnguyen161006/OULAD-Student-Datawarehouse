"""
metabase_export.py
==================
Export 4 dashboards (Student Performance / Pipeline Health / Demographics / Modules)
từ Metabase đang chạy ra `metabase/dashboards/*.json` để version-control.

Mỗi JSON chứa: dashboard name + description + danh sách cards (với SQL native,
visualization settings, position trong grid). Database được tham chiếu bằng *tên*
(không phải ID) để có thể re-import vào instance Metabase mới.

Chạy:
    python scripts/metabase_export.py

ENV:
    METABASE_URL              (default: http://localhost:3000)
    METABASE_ADMIN_EMAIL
    METABASE_ADMIN_PASSWORD
"""

from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.request
from pathlib import Path

METABASE_URL = os.getenv("METABASE_URL", "http://localhost:3000")
ADMIN_EMAIL = os.getenv("METABASE_ADMIN_EMAIL", "vinhnguyen161006@gmail.com")
ADMIN_PASS = os.getenv("METABASE_ADMIN_PASSWORD", "oulad12345")

# Dashboard ID -> slug filename
EXPORT_TARGETS = [
    (2, "01_student_performance"),
    (3, "02_pipeline_health"),
    (4, "03_demographics_success"),
    (5, "04_module_performance"),
]


def req(method: str, path: str, body=None, session: str | None = None):
    url = METABASE_URL + path
    data = json.dumps(body).encode() if body is not None else None
    request = urllib.request.Request(url, data=data, method=method)
    request.add_header("Content-Type", "application/json")
    if session:
        request.add_header("X-Metabase-Session", session)
    try:
        with urllib.request.urlopen(request, timeout=30) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        sys.stderr.write(f"HTTP {e.code} on {method} {path}: {e.read().decode()[:300]}\n")
        raise


def main() -> None:
    print(f"Login to Metabase at {METABASE_URL} as {ADMIN_EMAIL}")
    session = req("POST", "/api/session", {"username": ADMIN_EMAIL, "password": ADMIN_PASS})["id"]

    # Map database IDs -> names
    dbs = {db["id"]: db["name"] for db in req("GET", "/api/database", session=session)["data"]}
    print(f"Databases: {dbs}")

    out_dir = Path(__file__).resolve().parent.parent / "metabase" / "dashboards"
    out_dir.mkdir(parents=True, exist_ok=True)

    for d_id, slug in EXPORT_TARGETS:
        try:
            dash = req("GET", f"/api/dashboard/{d_id}", session=session)
        except urllib.error.HTTPError as e:
            if e.code == 404:
                print(f"  [SKIP] dashboard id={d_id} không tồn tại")
                continue
            raise

        export = {
            "name": dash["name"],
            "description": dash.get("description") or "",
            "cards": [],
        }
        for dc in sorted(dash["dashcards"], key=lambda x: (x["row"], x["col"])):
            card = dc["card"]
            ds_query = card["dataset_query"]
            db_name = dbs.get(ds_query["database"], f"<unknown:{ds_query['database']}>")

            # Metabase v0.50+ dùng `stages`, version cũ hơn dùng `native.query`
            sql = None
            if "stages" in ds_query and ds_query["stages"]:
                sql = ds_query["stages"][0].get("native")
            elif "native" in ds_query:
                sql = ds_query["native"].get("query")
            if not sql:
                print(f"  [WARN] Card '{card['name']}' không có native SQL, bỏ qua")
                continue

            export["cards"].append({
                "name": card["name"],
                "display": card["display"],
                "database": db_name,
                "query": sql.strip(),
                "visualization_settings": card.get("visualization_settings", {}),
                "position": {
                    "row": dc["row"],
                    "col": dc["col"],
                    "size_x": dc["size_x"],
                    "size_y": dc["size_y"],
                },
            })

        out_file = out_dir / f"{slug}.json"
        with open(out_file, "w", encoding="utf-8") as f:
            json.dump(export, f, indent=2, ensure_ascii=False)
        print(f"  [OK] dashboard {d_id} -> {out_file.relative_to(out_dir.parent.parent)} ({len(export['cards'])} cards)")

    print("\nDone.")


if __name__ == "__main__":
    main()
