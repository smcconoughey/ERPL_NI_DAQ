#!/usr/bin/env python3
# DAQ Streamer - connects NI-DAQmx to Node.js via TCP

import asyncio
import json
import socket
import time
import logging
import threading
from typing import AsyncGenerator, Dict, Any
import nidaqmx
from config import DAQ_HOST, DAQ_GRPC_PORT, NODE_HOST, NODE_TCP_PORT, DEVICE_CHASSIS, ACTIVE_DEVICES
from devices.device_registry import DeviceRegistry

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DAQStreamer:
    def __init__(self):
        self.node_host = NODE_HOST
        self.node_port = NODE_TCP_PORT
        self.running = False
        self.devices = []
        
        # Initialize active devices
        self._initialize_devices()
        
    def _initialize_devices(self):
        """Initialize configured devices"""
        for device_name in ACTIVE_DEVICES:
            try:
                device = DeviceRegistry.create_device(device_name, DEVICE_CHASSIS, module_slot=1)
                self.devices.append(device)
                logger.info(f"Initialized {device.device_info['device_type']}: {device.module_name}")
            except ValueError as e:
                logger.error(f"Failed to initialize device {device_name}: {e}")
                
    def stream_daq_data_sync(self) -> None:
        """Stream data from all configured devices (synchronous version)"""
        for device in self.devices:
            # Clear any existing tasks that might be using the device
            try:
                import nidaqmx.system
                system = nidaqmx.system.System.local()
                # Reset the device to clear any reserved resources
                device_name = device.module_name.split('/')[0]  # Get chassis name
                system.devices[device_name].reset_device()
                logger.info(f"Reset device {device_name} to clear reserved resources")
                time.sleep(1)  # Allow device to reset
            except Exception as e:
                logger.warning(f"Could not reset device: {e}")
                
            with nidaqmx.Task() as task:
                try:
                    # Configure device channels and timing
                    device.configure_channels(task)
                    device.configure_timing(task)
                    
                    logger.info(f"Starting {device.device_info['device_type']} on {device.module_name}")
                    # Don't start task here - start/stop for each finite acquisition
                    
                    while self.running:
                        try:
                            # Start finite acquisition
                            task.start()
                            
                            # Read all samples from finite acquisition - specify format
                            raw_data = task.read(
                                number_of_samples_per_channel=nidaqmx.constants.READ_ALL_AVAILABLE,
                                timeout=2.0
                            )
                            
                            # Stop task for next iteration
                            task.stop()
                            
                            # Handle data format - ensure we have list of lists for multi-channel
                            logger.info(f"Raw data type: {type(raw_data)}, length: {len(raw_data) if hasattr(raw_data, '__len__') else 'N/A'}")
                            
                            if isinstance(raw_data, (int, float)):
                                # Single value - create 16 channels with same value
                                raw_data = [[raw_data] for _ in range(device.channel_count)]
                            elif isinstance(raw_data, list):
                                if len(raw_data) == 0:
                                    # Empty list - create 16 channels with zeros
                                    raw_data = [[0.0] for _ in range(device.channel_count)]
                                elif not isinstance(raw_data[0], list):
                                    # Flat list - reshape for 16 channels
                                    if len(raw_data) == device.channel_count:
                                        # One sample per channel
                                        raw_data = [[val] for val in raw_data]
                                    elif len(raw_data) % device.channel_count == 0:
                                        # Multiple samples per channel - interleaved
                                        samples_per_chan = len(raw_data) // device.channel_count
                                        raw_data = [raw_data[i::device.channel_count] for i in range(device.channel_count)]
                                    else:
                                        # Odd length - distribute evenly and pad
                                        vals_per_chan = len(raw_data) // device.channel_count + 1
                                        raw_data = [raw_data[i:i+vals_per_chan] if i < len(raw_data) else [0.0] 
                                                   for i in range(0, device.channel_count)]
                            else:
                                # Unknown type - create default
                                raw_data = [[0.0] for _ in range(device.channel_count)]
                            
                            # Process data through device-specific handler
                            processed_data = device.process_data(raw_data)
                            processed_data["timestamp"] = time.time()
                            
                            # Send to Node.js server asynchronously
                            loop = asyncio.new_event_loop()
                            asyncio.set_event_loop(loop)
                            loop.run_until_complete(self.send_to_node(processed_data))
                            loop.close()
                            
                            # Wait before next finite acquisition (100ms = 10Hz update rate)
                            time.sleep(0.1)
                            
                        except Exception as e:
                            logger.error(f"Error reading from {device.module_name}: {e}")
                            try:
                                task.stop()
                            except:
                                pass
                            time.sleep(1)
                            continue
                            
                except Exception as e:
                    logger.error(f"Error configuring {device.module_name}: {e}")
                    continue
    
    async def send_to_node(self, data: Dict[str, Any]) -> None:
        """Send JSON data to Node.js TCP port"""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect((self.node_host, self.node_port))
                message = json.dumps(data) + '\n'
                sock.sendall(message.encode('utf-8'))
        except Exception as e:
            logger.error(f"Failed to send data to Node.js: {e}")
    
    def run(self) -> None:
        self.running = True
        logger.info("Starting DAQ streaming...")
        logger.info(f"Active devices: {[d.device_info['device_type'] for d in self.devices]}")
        
        if not self.devices:
            logger.error("No devices configured. Check config.py ACTIVE_DEVICES setting.")
            return
        
        try:
            self.stream_daq_data_sync()
                
        except KeyboardInterrupt:
            logger.info("Stopping DAQ streaming...")
        finally:
            self.running = False
    
    def stop(self) -> None:
        self.running = False

def main():
    streamer = DAQStreamer()
    
    try:
        streamer.run()
    except KeyboardInterrupt:
        logger.info("Received interrupt signal")
    finally:
        streamer.stop()

if __name__ == "__main__":
    main() 