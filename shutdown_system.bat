@echo off
echo.
echo ================================
echo   DAQ System Shutdown
echo ================================
echo.

echo Stopping DAQ services...

REM Kill Node.js processes (DAQ WebSocket Server)
echo Stopping Node.js WebSocket Server...
taskkill /f /im node.exe >nul 2>&1
if %errorlevel% equ 0 (
    echo ✓ Node.js WebSocket Server stopped
) else (
    echo ✓ No Node.js processes running
)

REM Kill Python processes (DAQ Streamer)
echo Stopping Python DAQ Streamer...
taskkill /f /im python.exe >nul 2>&1
if %errorlevel% equ 0 (
    echo ✓ Python DAQ Streamer stopped
) else (
    echo ✓ No Python processes running
)

REM Kill any cmd windows that might be running our services
echo Cleaning up service windows...
taskkill /f /fi "WINDOWTITLE eq DAQ WebSocket Server*" >nul 2>&1
taskkill /f /fi "WINDOWTITLE eq DAQ Python Streamer*" >nul 2>&1

echo.
echo ================================
echo   All DAQ services stopped
echo ================================
echo.
echo The system has been shut down cleanly.
echo You can now safely close this window.
echo.
pause 