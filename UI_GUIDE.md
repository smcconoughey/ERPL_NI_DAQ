# NI DAQ Telemetry UI Guide

## Overview
The NI DAQ telemetry UI is designed for continuous data monitoring and logging of pressure transducers (PT) and load cells (LC).

## Key Features

### 1. Automatic Logging
- **Logging starts automatically** when the WebSocket connects to the server
- No manual start/stop required
- Data is continuously written to CSV files at 100Hz+

### 2. Create New Log File
- **Center button** between ABORT and EMERGENCY STOP
- Creates a new timestamped log file (format: `logs/MMDD/HHMM.csv`)
- Closes current log before starting new one
- Confirmation dialog prevents accidental file creation

### 3. Top Info Bar
Displays real-time system information:
- **Connection Status**: Green indicator when connected
- **Last Update**: Timestamp of most recent data
- **Update Rate**: Real-time data rate in Hz
- **Message Count**: Total messages received

### 4. Large Telemetry Display
Two main sections with auto-updating grids:

**Pressure Transducers (16 channels)**
- Large, readable values in PSI
- Color-coded status:
  - Normal: Gray background with blue border
  - Disconnected: Red background (shows "DISCONNECTED")
  - Unconfigured: Orange background (shows "UNCONFIGURED")
- Hover effects for better visibility

**Load Cells (4 channels)**
- Displays V/V (volts per volt) with 6 decimal precision
- Purple border to distinguish from PTs
- Same responsive grid layout

### 5. Safety Controls
- **ABORT** (Yellow, left): Emergency abort button
- **EMERGENCY STOP** (Red, right): Critical shutdown button

## Layout

```
┌─────────────────────────────────────────────────────┐
│  ABORT  │  CREATE NEW LOG FILE  │  EMERGENCY STOP   │ ← Header (60px)
├─────────────────────────────────────────────────────┤
│ Connection | Last Update | Update Rate | Messages   │ ← Info Bar
├─────────────────────────────────────────────────────┤
│                                                      │
│  PRESSURE TRANSDUCERS (16 channels)                 │
│  ┌─────┬─────┬─────┬─────┐                         │
│  │ PT0 │ PT1 │ PT2 │ PT3 │ ...                      │
│  │14.50│22.30│ 0.00│45.67│                          │
│  │ PSI │ PSI │ PSI │ PSI │                          │
│  └─────┴─────┴─────┴─────┘                         │
│                                                      │
│  LOAD CELLS (4 channels)                            │
│  ┌──────────┬──────────┬──────────┬──────────┐    │
│  │   LC0    │   LC1    │   LC2    │   LC3    │    │
│  │ 0.000123 │ 0.000245 │ 0.000000 │ 0.000090 │    │
│  │   V/V    │   V/V    │   V/V    │   V/V    │    │
│  └──────────┴──────────┴──────────┴──────────┘    │
│                                                      │
└─────────────────────────────────────────────────────┘
│ Status messages...                                  │ ← Bottom Status Bar
└─────────────────────────────────────────────────────┘
```

## Access

**URL**: http://localhost:3000/daq_ui.html

**After starting the system:**
```batch
start_system_main.bat
```

## Data Flow

```
Python DAQ     →  Node.js Server  →  Web UI
(daq_streamer)    (server.js)        (daq_ui.html)
    ↓                   ↓
Hardware            CSV Logger
Acquisition         (automatic)
```

## Logging Behavior

1. **On Connect**: Logging starts automatically after 1 second
2. **Continuous**: Data written to CSV as it arrives
3. **Create New File**: 
   - Stops current log
   - Creates new timestamped file
   - Resumes logging immediately
4. **On Disconnect**: 
   - Current log is closed
   - Auto-restart on reconnect

## File Naming Convention

**Format**: `logs/MMDD/HHMM.csv`
- **MMDD**: Month and day (e.g., `1015` for October 15)
- **HHMM**: Hour and minute (e.g., `1831` for 6:31 PM)
- **Duplicates**: Appends `_1`, `_2`, etc. (e.g., `1831_1.csv`)

## CSV Structure

**PT Card:**
```csv
timestamp,elapsed_ms,PT0_psi,PT1_psi,...,PT15_psi
2025-10-15T18:31:00.000Z,0,14.50,22.30,...,0.00
```

**LC Card:**
```csv
timestamp,elapsed_ms,LC0_vv,LC1_vv,LC2_vv,LC3_vv
2025-10-15T18:31:00.000Z,0,0.000123,0.000245,0.000000,0.000090
```

## Tips

- **Monitor Update Rate**: Should be around 10 Hz (limited by Python DAQ loop)
- **Check Connection**: Green indicator = good, gray = disconnected
- **Status Indicators**: Watch for "DISCONNECTED" or "UNCONFIGURED" sensors
- **Log Files**: Check `logs/` directory for all recorded data
- **Performance**: Grid auto-scales for different screen sizes

## Comparison to Original UI

**Original** (`index.html`):
- Manual logging controls
- Smaller sensor displays
- Basic layout

**New Telemetry UI** (`daq_ui.html`):
- Automatic continuous logging
- Large, prominent sensor displays
- Modern card-based layout
- Real-time stats in info bar
- Safety controls at top
- Professional monitoring interface

