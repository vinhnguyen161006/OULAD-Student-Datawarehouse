# Tạo thư mục nếu chưa có
New-Item -ItemType Directory -Force -Path "data\raw" | Out-Null

Write-Host "Bat dau tai du lieu OULAD tu Dropbox..." -ForegroundColor Cyan

# Tải file
$url = "https://www.dropbox.com/scl/fi/3gf9c6nh3cga3n5uq9w2b/oulad_data.zip?rlkey=704gysvb7oyqdw4tbrbhuuade&st=lv3hm6cj&dl=1"
try {
    Invoke-WebRequest -Uri $url -OutFile "data\oulad_data.zip" -ErrorAction Stop
} catch {
    Write-Host "LOI: Khong the tai file. Kiem tra ket noi mang hoac URL Dropbox." -ForegroundColor Red
    exit 1
}

# Kiểm tra file zip tồn tại và không rỗng
if (-not (Test-Path "data\oulad_data.zip") -or (Get-Item "data\oulad_data.zip").Length -eq 0) {
    Write-Host "LOI: File zip bi hong hoac rong." -ForegroundColor Red
    exit 1
}

Write-Host "Dang giai nen du lieu vao thu muc data\raw\..." -ForegroundColor Cyan

# Giải nén
try {
    Expand-Archive -Path "data\oulad_data.zip" -DestinationPath "data\raw" -Force -ErrorAction Stop
} catch {
    Write-Host "LOI: Khong the giai nen file zip." -ForegroundColor Red
    exit 1
}

Write-Host "Dang don dep file rac..." -ForegroundColor Cyan
Remove-Item -Path "data\oulad_data.zip" -Force

Write-Host "HOAN TAT! Du lieu da nam gon gang tai data\raw\" -ForegroundColor Green
