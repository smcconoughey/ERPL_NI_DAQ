@echo off
setlocal enableextensions
pushd "%~dp0"

echo.
echo ================================
echo   ERPL DAQ System Startup
echo ================================
echo.

rem Locate Node.js (handles cases where it's not on PATH)
set "NODE_EXE="
for /f "delims=" %%i in ('where node 2^>nul') do (
    set "NODE_EXE=%%i"
    goto :found_node
)

for %%p in ("C:\Program Files\nodejs\node.exe" "%USERPROFILE%\AppData\Local\Programs\nodejs\node.exe" "C:\Program Files (x86)\nodejs\node.exe") do (
    if exist %%p (
        set "NODE_EXE=%%p"
        goto :found_node
    )
)

echo ERROR: Node.js not found. Install from https://nodejs.org and retry.
goto :end

:found_node
for %%i in ("%NODE_EXE%") do set "NODE_DIR=%%~dpi"
set "PATH=%NODE_DIR%;%PATH%"
echo Using Node: %NODE_EXE%

rem Install Node dependencies on first run
if exist node_modules goto after_npm_install
echo Installing Node.js dependencies...
where npm >nul 2>&1
if errorlevel 1 goto no_npm
call npm install --no-fund --no-audit
goto after_npm_install

:no_npm
echo ERROR: npm not found. Cannot install dependencies automatically.
echo Please install Node.js from https://nodejs.org (includes npm) or add npm to PATH.
goto :end

:after_npm_install

echo Starting Node.js WebSocket Server...
echo Launching: "%NODE_EXE%" "%CD%\server.js"
start "DAQ WebSocket Server" "%NODE_EXE%" "%CD%\server.js"

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
echo Both services are running in separate windows
echo Use shutdown_system.bat to stop cleanly
echo.
echo Opening web interface...
start http://localhost:3000
echo.

:end
popd
endlocal
