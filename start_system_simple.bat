@echo off
echo.
echo ================================
echo   DAQ System - Quick Start
echo ================================
echo.

echo Starting Node.js WebSocket Server...
start "DAQ WebSocket Server" cmd /k "title DAQ WebSocket Server && node server.js"

echo Waiting 3 seconds for server startup...
timeout /t 3 /nobreak >nul

echo Starting Python DAQ Streamer...
start "DAQ Python Streamer" cmd /k "title DAQ Python Streamer && python daq_streamer.py"

echo.
echo ================================
echo   System Started
echo ================================
echo Web interface: http://localhost:3000
echo.
echo Both services are running in separate windows
echo Use shutdown_system.bat to stop cleanly
echo.
echo Opening web interface...
start http://localhost:3000
echo.
pause 