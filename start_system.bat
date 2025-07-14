@echo off
echo Starting DAQ Data Streaming System...
echo.

echo Starting Node.js WebSocket Server...
start "Node.js Server" cmd /k "npm start"

echo Waiting for Node.js server to start...
timeout /t 3 /nobreak >nul

echo Starting Python DAQ Streamer...
start "Python Streamer" cmd /k "python daq_streamer.py"

echo.
echo System started! 
echo Web interface: http://localhost:3000
echo.
echo Press any key to continue...
pause >nul 