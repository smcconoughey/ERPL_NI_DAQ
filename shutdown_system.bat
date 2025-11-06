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

REM Signal Python DAQ Streamer to shutdown cleanly
echo Signaling Python DAQ Streamer to shutdown...
powershell -Command "Set-Content -Path 'shutdown_daq.cmd' -Value 'shutdown'" 2>nul
if exist shutdown_daq.cmd (
    echo ✓ Shutdown signal sent
    timeout /t 2 /nobreak >nul
) else (
    echo ! Failed to create shutdown signal
)

REM Kill Python processes (DAQ Streamer) - more specific
echo Stopping Python DAQ Streamer...
taskkill /f /fi "IMAGENAME eq python.exe" /fi "WINDOWTITLE eq DAQ Python Streamer*" >nul 2>&1
taskkill /f /im python3.13.exe >nul 2>&1
if %errorlevel% equ 0 (
    echo ✓ Python DAQ Streamer stopped
) else (
    echo ✓ No Python processes running
)

REM Clean up shutdown command file
if exist shutdown_daq.cmd del shutdown_daq.cmd

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