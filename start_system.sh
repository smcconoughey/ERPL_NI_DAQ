#!/bin/bash

echo "Starting DAQ Data Streaming System..."
echo

echo "Starting Node.js WebSocket Server..."
npm start &
NODE_PID=$!

echo "Waiting for Node.js server to start..."
sleep 3

echo "Starting Python DAQ Streamer..."
python daq_streamer.py &
PYTHON_PID=$!

echo
echo "System started!"
echo "Web interface: http://localhost:3000"
echo "Node.js PID: $NODE_PID"
echo "Python PID: $PYTHON_PID"
echo
echo "Press Ctrl+C to stop both services..."

trap 'echo "Stopping services..."; kill $NODE_PID $PYTHON_PID; exit' INT
wait 