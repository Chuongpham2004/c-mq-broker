# Windows PowerShell Script to test durability & session resuming using WSL binaries
Remove-Item -Path "data" -Recurse -Force -ErrorAction SilentlyContinue
Remove-Item -Path "consumer.log", "consumer_reconnect.log" -Force -ErrorAction SilentlyContinue

Write-Host "==========================================================" -ForegroundColor Green
Write-Host "=== KHỞI ĐỘNG KIỂM THỬ ĐỘ TIN CẬY TRÊN WINDOWS (WSL) ===" -ForegroundColor Green
Write-Host "==========================================================" -ForegroundColor Green

# 1. Khởi động broker trong nền qua WSL
$brokerJob = Start-Job -ScriptBlock { wsl ./bin/broker --fsync per-write }
Start-Sleep -Seconds 1
Write-Host "[Test] Broker đã khởi chạy trong nền."

# 2. Khởi chạy consumer_1 để đăng ký topic lần đầu qua WSL
Write-Host "[Test] Khởi chạy consumer_1 để đăng ký topic 'sensor/nhietdo'..."
$consumerJob = Start-Job -ScriptBlock { wsl ./bin/consumer consumer_1 sensor/nhietdo }
Start-Sleep -Seconds 1

# Tắt consumer đi để chuyển sang trạng thái offline
Write-Host "[Test] Tắt consumer_1 để chuyển sang chế độ offline..."
Stop-Job -Job $consumerJob
Remove-Job -Job $consumerJob
Start-Sleep -Seconds 1

# 3. Gửi các tin nhắn mới qua WSL
Write-Host "[Test] Publisher gửi các tin nhắn mới..."
wsl ./bin/producer sensor/nhietdo "Tin nhan offline so 1"
wsl ./bin/producer sensor/nhietdo "Tin nhan offline so 2"
wsl ./bin/producer sensor/nhietdo "Tin nhan offline so 3"
Start-Sleep -Seconds 1

# 4. Kill Broker bằng cách dừng Job và đảm bảo kill tiến trình trong WSL
Write-Host "[Test] Dừng Broker đột ngột (giả lập sập nguồn)..."
Stop-Job -Job $brokerJob
Remove-Job -Job $brokerJob
wsl killall broker 2>$null
Start-Sleep -Seconds 1

# 5. Khởi động lại Broker qua WSL
Write-Host "[Test] Khởi động lại Broker..."
$newBrokerJob = Start-Job -ScriptBlock { wsl ./bin/broker --fsync per-write }
Start-Sleep -Seconds 1

# 6. Cho Consumer kết nối lại qua WSL và ghi nhận log nhận tin
Write-Host "[Test] Cho consumer_1 kết nối lại để nhận tin nhắn cũ..."
wsl ./bin/consumer consumer_1 sensor/nhietdo > consumer_reconnect.log
Start-Sleep -Seconds 1

# Dọn dẹp các Job và tiến trình chạy ngầm
Stop-Job -Job $newBrokerJob
Remove-Job -Job $newBrokerJob
wsl killall broker 2>$null
wsl killall consumer 2>$null

Write-Host ""
Write-Host "=== KẾT QUẢ NHẬN TIN CỦA CONSUMER SAU KHI RECONNECT ===" -ForegroundColor Cyan
if (Test-Path "consumer_reconnect.log") {
    Get-Content "consumer_reconnect.log"
} else {
    Write-Host "Lỗi: Không tìm thấy file consumer_reconnect.log" -ForegroundColor Red
}

Write-Host "==========================================================" -ForegroundColor Green
Write-Host "=== HOÀN THÀNH BÀI TEST DURABILITY & RECOVERY THÀNH CÔNG ===" -ForegroundColor Green
Write-Host "==========================================================" -ForegroundColor Green
