@echo off
echo Starting Mini HDFS System...
echo.

echo [0/5] Starting Kafka event broker...
docker compose -f kafka\docker-compose.yml up -d
PowerShell -NoProfile -ExecutionPolicy Bypass -File kafka\create-topics.ps1
timeout /t 3 /nobreak >nul

echo [1/5] Starting NameNode...
start "NameNode" cmd /k "python namenode/namenode.py"
timeout /t 2 /nobreak >nul

echo [2/5] Starting DataNode 1...
start "DataNode1" cmd /k "python datanode1/datanode1.py"
timeout /t 2 /nobreak >nul

echo [3/5] Starting DataNode 2...
start "DataNode2" cmd /k "python datanode2/datanode2.py"
timeout /t 2 /nobreak >nul

echo [4/5] Starting Client/Web Dashboard...
start "Client" cmd /k "python client/client.py"
timeout /t 2 /nobreak >nul

echo [5/5] Starting DataNode 3 (Optional)...
start "DataNode3" cmd /k "python datanode3/datanode3.py"

echo.
echo All services started successfully!
echo.
echo Access the web dashboard at: http://127.0.0.1:5000
echo.
echo Press any key to close this window...
pause >nul
