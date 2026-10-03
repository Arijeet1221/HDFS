@echo off
echo Stopping Mini HDFS System...
echo.

taskkill /FI "WINDOWTITLE eq NameNode*" /F >nul 2>&1
taskkill /FI "WINDOWTITLE eq DataNode1*" /F >nul 2>&1
taskkill /FI "WINDOWTITLE eq DataNode2*" /F >nul 2>&1
taskkill /FI "WINDOWTITLE eq DataNode3*" /F >nul 2>&1
taskkill /FI "WINDOWTITLE eq Client*" /F >nul 2>&1

echo All services stopped.
echo.
