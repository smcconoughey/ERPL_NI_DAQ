# NI DAQ Logging System

## Overview
The NI DAQ system now includes comprehensive CSV logging functionality at 100Hz+ with timestamped log files.

## Features

### 1. Automatic File Naming
- Log files are created in `logs/MMDD/HHMM.csv` format
- Example: `logs/1014/1831.csv` for a file created at 6:31 PM on October 14th
- If a file already exists, a counter is appended: `1831_1.csv`, `1831_2.csv`, etc.

### 2. CSV Format
Each log file contains:
- **Header row**: `timestamp`, `elapsed_ms`, followed by channel columns
  - PT Card: `PT0_psi` through `PT15_psi` (16 channels)
  - LC Card: `LC0_vv` through `LC3_vv` (4 channels V/V)
- **Data rows**: ISO timestamp, milliseconds since start, followed by sensor values

### 3. Logging Rate
- Minimum 100Hz logging rate
- Data is flushed to disk every 10 rows to ensure durability
- Synchronized with DAQ acquisition rate

## Using the Logging System

### From the Web UI (daq_ui.html)

1. **Start Logging**
   - Click the "Start Logging" button in the Data Logging panel
   - A success notification will appear with the filename
   - The logging indicator will turn green and pulse

2. **Stop Logging**
   - Click the "Stop Logging" button
   - A notification will show the number of rows written
   - The indicator will turn gray

3. **Monitor Status**
   - The current log filename is displayed in the panel
   - Status updates every 2 seconds automatically

### Accessing the UI

Open your browser to:
- **New Telemetry Monitor**: http://localhost:3000/daq_ui.html
- **Original Interface**: http://localhost:3000/index.html

## Technical Implementation

### Server-Side (server.js)
The Node.js WebSocket server handles logging:
- `startLogging()`: Creates timestamped CSV file
- `stopLogging()`: Closes log file and reports statistics
- `getLoggingStatus()`: Returns current logging state
- `writeLogEntry(data)`: Writes data rows to CSV

### WebSocket Commands
Clients can send these actions via WebSocket:
```javascript
{ action: 'start_logging' }
{ action: 'stop_logging' }
{ action: 'get_logging_status' }
```

### Log File Structure

**PT Card Log Example:**
```csv
timestamp,elapsed_ms,PT0_psi,PT1_psi,...,PT15_psi
2025-10-15T18:31:00.000Z,0,0.00,0.00,...,0.00
2025-10-15T18:31:00.010Z,10,14.5,22.3,...,0.00
```

**LC Card Log Example:**
```csv
timestamp,elapsed_ms,LC0_vv,LC1_vv,LC2_vv,LC3_vv
2025-10-15T18:31:00.000Z,0,0.000001,0.000002,0.000000,0.000001
2025-10-15T18:31:00.010Z,10,0.000123,0.000245,0.000050,0.000090
```

## Log File Location

All logs are stored in the `ERPL_NI_DAQ/logs/` directory:
```
ERPL_NI_DAQ/
  logs/
    1014/          # October 14
      1831.csv     # Created at 6:31 PM
      1831_1.csv   # Second log started at 6:31 PM
      1905.csv     # Created at 7:05 PM
    1015/          # October 15
      0830.csv
```

## System Architecture

```
┌─────────────────┐
│  Python DAQ     │ Reads hardware at 10Hz
│  (daq_streamer) │ (500 samples/channel per read)
└────────┬────────┘
         │ TCP (port 5001)
         │ JSON data
         ▼
┌─────────────────┐
│  Node.js Server │ Receives data, logs to CSV
│  (server.js)    │ Broadcasts via WebSocket
└────────┬────────┘
         │ WebSocket (port 3000)
         │ JSON data + commands
         ▼
┌─────────────────┐
│  Web UI         │ Display + logging controls
│  (daq_ui.html)  │
└─────────────────┘
```

## Notes

- Logging continues even if UI disconnects
- Multiple UIs can connect; logging state is shared
- CSV files use UTF-8 encoding
- Empty/missing values are written as empty strings in CSV
- The system gracefully handles errors and continues operation

