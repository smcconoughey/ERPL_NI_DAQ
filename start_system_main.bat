@echo off
echo.
echo ================================
echo   ERPL DAQ System Startup
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
echo   System Started Successfully
echo ================================
echo.
echo Web Interface:     http://localhost:3000
echo DAQ Hardware:      192.168.8.236 (cDAQ9189-2462EFD)
echo WebSocket Port:    3000
echo TCP Data Port:     5001
echo.
echo Interface Features:
echo   - 70% P&ID diagram area (placeholder)
echo   - 30% live sensor data (grouped by system)
echo   - High-contrast light mode for outdoor viewing
echo   - 16-channel PT monitoring (4-20mA to ksi)
echo.
echo Both services are running in separate windows
echo Use shutdown_system.bat to stop cleanly
echo.
echo Opening web interface...
start http://localhost:3000
echo.
pause 