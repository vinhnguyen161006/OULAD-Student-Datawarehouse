# Dashboard Screenshots

Thư mục chứa screenshot 4 Metabase dashboards để hiển thị trong README chính.

## Hướng dẫn capture

1. **Mở Metabase**: http://localhost:3000
2. **Đăng nhập** với credential đã tạo (xem CLAUDE.md / lịch sử setup)
3. **Vào từng dashboard** và chụp full-page screenshot:

| File cần lưu | Dashboard URL |
|---|---|
| `01_student_performance.png` | http://localhost:3000/dashboard/2 |
| `02_pipeline_health.png` | http://localhost:3000/dashboard/3 |
| `03_demographics.png` | http://localhost:3000/dashboard/4 |
| `04_modules.png` | http://localhost:3000/dashboard/5 |

## Cách chụp full-page trên Chrome/Edge

1. `F12` mở DevTools
2. `Ctrl+Shift+P` → gõ "Capture full size screenshot" → Enter
3. Lưu file vào `docs/screenshots/<tên>.png`

## Lưu ý

- Resize browser về 1440px width trước khi chụp để screenshot không quá to.
- File PNG kích thước ~300-800 KB là vừa đẹp; nếu lớn hơn 1MB hãy nén bằng [tinypng.com](https://tinypng.com).
- Tên file phải khớp đúng với reference trong [README.md](../../README.md).
