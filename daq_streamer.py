#!/usr/bin/env python3
# DAQ Streamer - connects NI-DAQmx to Node.js via gRPC and TCP

import grpc
import asyncio
import json
import socket
import time
import logging
from typing import AsyncGenerator, Dict, Any
from nidaqmx.grpc_device_client import create_channel, AnalogInputReader

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DAQStreamer:
    def __init__(self, grpc_host: str = "localhost", grpc_port: int = 31763, 
                 node_host: str = "localhost", node_port: int = 5000):
        self.grpc_host = grpc_host
        self.grpc_port = grpc_port
        self.node_host = node_host
        self.node_port = node_port
        self.running = False
        
    async def connect_to_daq(self) -> AsyncGenerator[Dict[str, Any], None]:
        """Stream samples from NI-DAQmx gRPC"""
        channel = create_channel(f"{self.grpc_host}:{self.grpc_port}")
        reader = AnalogInputReader(channel)
        
        logger.info(f"Connected to DAQ at {self.grpc_host}:{self.grpc_port}")
        
        async for sample in reader.read_samples_async():
            if not self.running:
                break
            yield sample
    
    async def send_to_node(self, data: Dict[str, Any]) -> None:
        """Send JSON data to Node.js TCP port"""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect((self.node_host, self.node_port))
                message = json.dumps(data) + '\n'
                sock.sendall(message.encode('utf-8'))
        except Exception as e:
            logger.error(f"Failed to send data to Node.js: {e}")
    
    async def run(self) -> None:
        self.running = True
        logger.info("Starting DAQ streaming...")
        
        try:
            async for sample in self.connect_to_daq():
                if not self.running:
                    break
                await self.send_to_node(sample)
                
        except KeyboardInterrupt:
            logger.info("Stopping DAQ streaming...")
        finally:
            self.running = False
    
    def stop(self) -> None:
        self.running = False

async def main():
    streamer = DAQStreamer()
    
    try:
        await streamer.run()
    except KeyboardInterrupt:
        logger.info("Received interrupt signal")
    finally:
        streamer.stop()

if __name__ == "__main__":
    asyncio.run(main()) 