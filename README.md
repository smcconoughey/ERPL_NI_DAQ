# DAQ Data Streaming System

A real-time data streaming system that connects NI-DAQmx hardware to web browsers via Python gRPC and Node.js WebSockets.

## Architecture

```
[DAQ + NI-DAQmx] → [Python gRPC Server] → [Node.js WebSocket Server] → [Browser Clients]
```

## Features

- Real-time data streaming from NI-DAQmx via gRPC
- WebSocket communication for low-latency browser updates
- Modern web interface with live data visualization
- Automatic reconnection and error handling
- Simulated data mode for testing without hardware

## Setup

### Python Environment (using pyenv)

1. Install pyenv if not already installed
2. Install Python 3.11.0:
   ```bash
   pyenv install 3.11.0
   pyenv local 3.11.0
   ```

3. Install Python dependencies:
   ```bash
   pip install -r requirements.txt
   ```

### Node.js Environment

1. Install Node.js dependencies:
   ```bash
   npm install
   ```

## Usage

### Start the Node.js WebSocket Server

```bash
npm start
```

The web interface will be available at `http://localhost:3000`

### Start the Python DAQ Streamer

```bash
python daq_streamer.py
```

The system will:
- Attempt to connect to NI-DAQmx gRPC service on localhost:31763
- Fall back to simulated data if DAQ hardware is not available
- Send data to the Node.js server via TCP on port 5000

## Configuration

### Python DAQ Streamer (`daq_streamer.py`)

- `grpc_host`: NI-DAQmx gRPC server host (default: localhost)
- `grpc_port`: NI-DAQmx gRPC server port (default: 31763)
- `node_host`: Node.js server host (default: localhost)
- `node_port`: Node.js TCP port (default: 5000)

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

1. **Connection refused**: Start Node.js server before Python streamer
2. **DAQ not found**: Check NI-DAQmx gRPC service on specified port
3. **WebSocket connection failed**: Check firewall and server accessibility

## License

MIT 