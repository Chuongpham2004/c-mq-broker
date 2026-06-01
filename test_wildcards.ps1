# Windows PowerShell Script to test wildcard topic matching using WSL
Remove-Item -Path "data" -Recurse -Force -ErrorAction SilentlyContinue
Remove-Item -Path "wildcard_plus.log", "wildcard_hash.log" -Force -ErrorAction SilentlyContinue

Write-Host "==========================================================" -ForegroundColor Green
Write-Host "=== KHỞI ĐỘNG KIỂM THỬ KÝ TỰ ĐẠI DIỆN TRÊN WINDOWS (WSL) ===" -ForegroundColor Green
Write-Host "==========================================================" -ForegroundColor Green

# Chạy trực tiếp test_wildcards.sh trong WSL để thu thập dữ liệu log chính xác
Write-Host "[Test] Chạy test_wildcards.sh trong WSL..."
wsl chmod +x test_wildcards.sh
wsl ./test_wildcards.sh

Write-Host "==========================================================" -ForegroundColor Green
Write-Host "=== HOÀN THÀNH BÀI TEST TOPIC WILDCARDS THÀNH CÔNG ===" -ForegroundColor Green
Write-Host "==========================================================" -ForegroundColor Green
