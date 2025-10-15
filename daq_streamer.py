#!/usr/bin/env python3
# DAQ Streamer - connects NI-DAQmx to Node.js via TCP

import asyncio
import json
import socket
import time
import logging
import threading
import csv
from datetime import datetime
from pathlib import Path
from typing import AsyncGenerator, Dict, Any
import nidaqmx
from config import (
    DAQ_HOST,
    DAQ_GRPC_PORT,
    NODE_HOST,
    NODE_TCP_PORT,
    DEVICE_CHASSIS,
    ACTIVE_DEVICES,
    MODULE_SLOT,
    DEBUG_ENABLE,
    DEBUG_RAW_SUMMARY,
    DEBUG_RAW_SAMPLES,
    DEBUG_SAMPLE_EVERY_N,
)
from devices.device_registry import DeviceRegistry

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DAQStreamer:
    def __init__(self):
        self.node_host = NODE_HOST
        self.node_port = NODE_TCP_PORT
        self.running = False
        self.devices = []
        
        # CSV logging
        self.logging_enabled = False
        self.log_file = None
        self.log_writer = None
        self.log_filename = None
        self.log_lock = threading.Lock()
        self.log_data_count = 0
        self.log_start_time = None
        
        # Initialize active devices
        self._initialize_devices()
        
    def _initialize_devices(self):
        """Initialize configured devices"""
        for device_name in ACTIVE_DEVICES:
            try:
                device = DeviceRegistry.create_device(device_name, DEVICE_CHASSIS, module_slot=MODULE_SLOT)
                self.devices.append(device)
                logger.info(f"Initialized {device.device_info['device_type']}: {device.module_name}")
                # Warmup details
                try:
                    info = getattr(device, 'device_info', {})
                    product = getattr(device, 'product_type', 'Unknown')
                    logger.info(f"Module selected: {device.module_name} ({product})")
                    if hasattr(device, 'sensor_config'):
                        logger.info(f"Sensors configured: {len(getattr(device, 'sensor_config', {}))}")
                except Exception:
                    pass
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
                # Prefer resetting the chassis; fall back to the module if needed
                target_names = [device.chassis, device.module_name]
                reset_done = False
                for name in target_names:
                    try:
                        system.devices[name].reset_device()
                        logger.info(f"Reset device {name} to clear reserved resources")
                        reset_done = True
                        break
                    except Exception:
                        continue
                if not reset_done:
                    available = [d.name for d in system.devices]
                    logger.warning(f"Could not reset device. Tried {target_names}. Available: {available}")
                else:
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
                            
                            # Optional raw debug logs
                            if 'read_count' not in locals():
                                read_count = 0
                            read_count += 1
                            if DEBUG_ENABLE and isinstance(raw_data, list) and raw_data:
                                if DEBUG_RAW_SUMMARY and (read_count % max(1, DEBUG_SAMPLE_EVERY_N) == 0):
                                    max_channels = min(8, len(raw_data))
                                    for ch in range(max_channels):
                                        samples = raw_data[ch]
                                        if not samples:
                                            continue
                                        avg_a = sum(samples) / len(samples)
                                        min_a = min(samples)
                                        max_a = max(samples)
                                        logger.info(
                                            f"ch{ch:02d} A(avg/min/max)="
                                            f"{avg_a:.6f}/{min_a:.6f}/{max_a:.6f}  "
                                            f"mA(avg)={(avg_a*1000):.3f}  n={len(samples)}"
                                        )
                                if DEBUG_RAW_SAMPLES and (read_count % max(1, DEBUG_SAMPLE_EVERY_N) == 0):
                                    for ch in range(min(2, len(raw_data))):
                                        samples = raw_data[ch][:10]
                                        logger.info(f"ch{ch:02d} first samples (A): {samples}")
                            
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

                            # Ensure exactly 16 channels
                            while len(raw_data) < device.channel_count:
                                raw_data.append([0.0])
                            raw_data = raw_data[:device.channel_count]
                            
                            # Process data through device-specific handler
                            processed_data = device.process_data(raw_data)
                            processed_data["timestamp"] = time.time()
                            
                            # Log data if enabled
                            if self.logging_enabled:
                                try:
                                    self._write_log_entry(processed_data)
                                except Exception as e:
                                    logger.error(f"Failed to write log entry: {e}")
                            
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
    
    def start_logging(self) -> Dict:
        """Start CSV logging with timestamped filename"""
        if self.logging_enabled:
            return {"success": False, "message": "Logging already active", "filename": self.log_filename}
        
        try:
            # Create folder named with date (MMDD)
            now = datetime.now()
            date_folder = now.strftime("%m%d")
            log_dir = Path("logs") / date_folder
            log_dir.mkdir(parents=True, exist_ok=True)
            
            # Create filename with time (HHMM.csv)
            time_str = now.strftime("%H%M")
            log_path = log_dir / f"{time_str}.csv"
            
            # If file exists, append a number
            counter = 1
            while log_path.exists():
                log_path = log_dir / f"{time_str}_{counter}.csv"
                counter += 1
            
            self.log_file = open(log_path, 'w', newline='')
            self.log_writer = csv.writer(self.log_file)
            
            # Write header dynamically based on active devices
            header = ['timestamp', 'elapsed_ms']
            
            for device in self.devices:
                device_type = device.device_info.get('device_type', '')
                
                if 'PT' in device_type or 'Pressure' in device_type:
                    # PT Card has 16 channels
                    for i in range(16):
                        header.append(f'PT{i}_psi')
                elif 'LC' in device_type or 'Load' in device_type:
                    # LC Card has 4 channels
                    for i in range(4):
                        header.append(f'LC{i}_vv')
            
            self.log_writer.writerow(header)
            self.log_file.flush()
            
            self.logging_enabled = True
            self.log_filename = str(log_path)
            self.log_data_count = 0
            self.log_start_time = time.time()
            
            logger.info(f"Started CSV logging: {self.log_filename}")
            return {"success": True, "message": "Logging started", "filename": self.log_filename}
        
        except Exception as e:
            logger.error(f"Failed to start logging: {e}")
            if self.log_file:
                self.log_file.close()
                self.log_file = None
            return {"success": False, "message": str(e)}
    
    def stop_logging(self) -> Dict:
        """Stop CSV logging"""
        if not self.logging_enabled:
            return {"success": False, "message": "Logging not active"}
        
        try:
            self.logging_enabled = False
            if self.log_file:
                self.log_file.close()
                self.log_file = None
            
            logger.info(f"Stopped CSV logging. Wrote {self.log_data_count} rows to {self.log_filename}")
            filename = self.log_filename
            rows = self.log_data_count
            self.log_filename = None
            self.log_writer = None
            self.log_data_count = 0
            
            return {"success": True, "message": "Logging stopped", "rows": rows, "filename": filename}
        
        except Exception as e:
            logger.error(f"Failed to stop logging: {e}")
            return {"success": False, "message": str(e)}
    
    def get_logging_status(self) -> Dict:
        """Get current logging status"""
        return {
            "active": self.logging_enabled,
            "filename": self.log_filename if self.logging_enabled else None,
            "rows": self.log_data_count if self.logging_enabled else 0,
            "elapsed_sec": (time.time() - self.log_start_time) if self.log_start_time else 0
        }
    
    def _write_log_entry(self, processed_data: Dict[str, Any]):
        """Write current data to CSV log"""
        if not self.logging_enabled or self.log_writer is None:
            return
        
        with self.log_lock:
            timestamp = datetime.now().isoformat()
            elapsed_ms = int((time.time() - self.log_start_time) * 1000) if self.log_start_time else 0
            
            row = [timestamp, elapsed_ms]
            
            # Extract data from processed_data based on device type
            if 'channels' in processed_data:
                channels = processed_data['channels']
                
                # Determine device type from first channel
                if channels and len(channels) > 0:
                    first_ch = channels[0]
                    
                    if 'pressure_psi' in first_ch:
                        # PT data (16 channels)
                        for i in range(16):
                            if i < len(channels):
                                row.append(channels[i].get('pressure_psi', ''))
                            else:
                                row.append('')
                    elif 'v_per_v' in first_ch:
                        # LC data (4 channels)
                        for i in range(4):
                            if i < len(channels):
                                row.append(channels[i].get('v_per_v', ''))
                            else:
                                row.append('')
            
            self.log_writer.writerow(row)
            self.log_data_count += 1
            
            # Flush every 10 rows to ensure data is written
            if self.log_data_count % 10 == 0:
                self.log_file.flush()
    
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