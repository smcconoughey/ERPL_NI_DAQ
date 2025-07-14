# DAQ Data Streaming System

A real-time data streaming system that connects NI-DAQmx hardware to web browsers via Python gRPC and Node.js WebSockets.

## Architecture

```
[DAQ + NI-DAQmx] → [Python gRPC Server] → [Node.js WebSocket Server] → [Browser Clients]
```

## Features

- Real-time data streaming from NI-DAQmx hardware
- WebSocket communication for low-latency browser updates
- Modern web interface with live data visualization
- Modular device configuration system
- 16-channel PT card (NI-9208) current monitoring
- Automatic reconnection and error handling
- Extensible device registry for future hardware

## Setup

### Option 1: Python Virtual Environment (Recommended)

1. **Set up Python environment:**
   ```bash
   .\setup_python_env.bat
   ```

   This will:
   - Auto-detect if you have pyenv-win or system Python
   - Create a virtual environment (`venv/`)
   - Install all Python dependencies in isolation
   - Set up the proper environment for DAQ development

2. **If you want pyenv-win for better Python management:**
   ```bash
   .\install_pyenv_win.bat
   ```

   Then restart terminal and run `setup_python_env.bat`

### Option 2: System Python (Basic Setup)

1. **Run the basic setup script:**
   ```bash
   .\setup.bat
   ```

   This will:
   - Check if Python and Node.js are installed
   - Install all dependencies globally
   - Verify system requirements

### Prerequisites

- **Python**: Download from [python.org](https://python.org/downloads) 
  - ⚠️ **IMPORTANT**: Check "Add Python to PATH" during installation
- **Node.js**: Download from [nodejs.org](https://nodejs.org)

### Manual Setup

1. **Install Python dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Install Node.js dependencies:**
   ```bash
   npm install
   ```

## Usage

**Start System:**
```bash
.\start_system.bat
```

**Shutdown System:**
```bash
.\shutdown_system.bat
```

**Development Mode:**
```bash
.\dev_start.bat
```

### Manual Start

1. **Activate virtual environment (if using):**
   ```bash
   call venv\Scripts\activate.bat
   ```

2. **Start Node.js server:**
   ```bash
   npm start
   ```

3. **Start Python streamer:**
   ```bash
   python daq_streamer.py
   ```

**Web Interface:** `http://localhost:3000`

The system will:
- Connect to NI-DAQmx hardware at 192.168.8.236
- Stream 16-channel current data from PT card (NI-9208)
- Send data to the Node.js server via TCP on port 5000

## Configuration

### Configuration (`config.py`)

- `DAQ_HOST`: NI-DAQmx hardware IP address (default: 192.168.8.236)
- `NODE_HOST`: Node.js server host (default: localhost)
- `NODE_TCP_PORT`: Node.js TCP port (default: 5000)
- `DEVICE_CHASSIS`: DAQ chassis name (default: cDAQ9189)
- `ACTIVE_DEVICES`: List of active device modules (default: ["pt_card"])

### Adding New Devices

1. Create new device class in `devices/` extending `BaseDevice`
2. Register device in `devices/device_registry.py`
3. Add to `ACTIVE_DEVICES` list in `config.py`

### Node.js WebSocket Server (`server.js`)

- `WEB_PORT`: Web interface port (default: 3000)
- `TCP_PORT`: TCP port for Python data (default: 5000)
- `MAX_CLIENTS`: Maximum WebSocket clients (default: 100)

## API Endpoints

- `GET /`: Web interface
- `GET /api/status`: Server status and statistics

## Development

### Run Node.js server in development mode:

```bash
npm run dev
```

## Troubleshooting

1. **"Cannot keep up with hardware acquisition"**: The system will automatically optimize buffer sizes and read rates
2. **"Resource is reserved"**: Another application may be using the DAQ device - close NI MAX or other DAQ software
3. **Connection refused**: Use start_system.bat which starts services in the correct order
4. **DAQ not found**: Check NI-DAQmx Runtime installation and hardware connection (IP: 192.168.8.236)
5. **WebSocket connection failed**: Check firewall and ensure no other services are using port 3000

### System Management

- **Start**: Run `start_system.bat` - it will check all dependencies and start services properly
- **Stop**: Run `shutdown_system.bat` - it will cleanly stop all DAQ services
- **Monitor**: Both services run in separate windows showing real-time status and errors

## License

MIT 