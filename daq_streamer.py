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
    PT_MODULE_SLOT,
    LC_MODULE_SLOT,
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
        
        # Last-known per-device channel snapshots for merged logging/UI
        self._last_pt_channels = []
        self._last_lc_channels = []

        # Persistent NI-DAQmx tasks (device -> task)
        self._device_tasks = []

        # Logging status file for UI/Node to read
        self.status_file = Path(__file__).parent / 'logging_status.json'
        
        # Tare command file
        self.tare_cmd_file = Path(__file__).parent / 'tare.cmd'  # legacy: tare all
        self.tare_lc_cmd_file = Path(__file__).parent / 'tare_lc.cmd'
        self.tare_pt_cmd_file = Path(__file__).parent / 'tare_pt.cmd'
        self.start_log_cmd = Path(__file__).parent / 'start_logging.cmd'
        self.stop_log_cmd = Path(__file__).parent / 'stop_logging.cmd'
        
        # Initialize active devices
        self._initialize_devices()
        
    def _initialize_devices(self):
        """Initialize configured devices"""
        for device_name in ACTIVE_DEVICES:
            try:
                # Use per-device slot hints when available
                slot = MODULE_SLOT
                if device_name == "pt_card":
                    slot = PT_MODULE_SLOT
                elif device_name == "lc_card":
                    slot = LC_MODULE_SLOT

                device = DeviceRegistry.create_device(device_name, DEVICE_CHASSIS, module_slot=slot)
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
        """Stream data from all configured devices at ~100 Hz using persistent tasks."""
        # Reset all devices once at startup
        try:
            import nidaqmx.system
            system = nidaqmx.system.System.local()
            for d in self.devices:
                target_names = [d.chassis, d.module_name]
                for name in target_names:
                    try:
                        system.devices[name].reset_device()
                        logger.info(f"Reset device {name} to clear reserved resources")
                        break
                    except Exception:
                        continue
            time.sleep(1)
        except Exception as e:
            logger.warning(f"Could not reset devices: {e}")

        logger.info(f"Starting DAQ devices: {[d.device_info['device_type'] for d in self.devices]}")

        # Create and start continuous tasks once
        self._device_tasks = []
        for device in self.devices:
            try:
                task = nidaqmx.Task()
                device.configure_channels(task)
                device.configure_timing(task)
                task.start()
                self._device_tasks.append((device, task))
                logger.info(f"Started {device.device_info['device_type']} on {device.module_name}")
            except Exception as e:
                logger.error(f"Failed to start device {device.device_info['device_type']}: {e}")

        per_device_read_count = {id(d): 0 for d, _ in self._device_tasks}

        target_period_s = 0.10  # 10 Hz
        while self.running:
            loop_t0 = time.time()
            # Handle start/stop logging requests from Node via command files
            try:
                if self.start_log_cmd.exists() and not self.logging_enabled:
                    self.start_logging()
                    try:
                        self.start_log_cmd.unlink()
                    except FileNotFoundError:
                        pass
                if self.stop_log_cmd.exists() and self.logging_enabled:
                    self.stop_logging()
                    try:
                        self.stop_log_cmd.unlink()
                    except FileNotFoundError:
                        pass
            except Exception as e:
                logger.error(f"Failed to process logging cmd: {e}")

            # Snapshot tare requests at top of cycle so we can apply across all devices
            do_tare_all = self.tare_cmd_file.exists()
            do_tare_lc = self.tare_lc_cmd_file.exists()
            do_tare_pt = self.tare_pt_cmd_file.exists()

            for device, task in self._device_tasks:
                try:
                    # Read a small window per cycle; non-blocking / short timeout
                    # Drain whatever is available up to our window; tolerate up to 100 ms
                    raw_data = task.read(
                        number_of_samples_per_channel=nidaqmx.constants.READ_ALL_AVAILABLE,
                        timeout=0.10
                    )

                    # Debug summaries occasionally
                    per_device_read_count[id(device)] += 1
                    if DEBUG_ENABLE and isinstance(raw_data, list) and raw_data:
                        if DEBUG_RAW_SUMMARY and (per_device_read_count[id(device)] % max(1, DEBUG_SAMPLE_EVERY_N) == 0):
                            max_channels = min(8, len(raw_data))
                            for ch in range(max_channels):
                                samples = raw_data[ch]
                                if not samples:
                                    continue
                                avg_a = sum(samples) / len(samples)
                                min_a = min(samples)
                                max_a = max(samples)
                                logger.info(
                                    f"{device.device_info['device_type']} ch{ch:02d} avg={avg_a:.6f} A  mA={(avg_a*1000):.3f}  n={len(samples)}"
                                )

                    # Normalize shape
                    if isinstance(raw_data, (int, float)):
                        raw_data = [[raw_data] for _ in range(device.channel_count)]
                    elif isinstance(raw_data, list):
                        if len(raw_data) == 0:
                            raw_data = [[0.0] for _ in range(device.channel_count)]
                        elif not isinstance(raw_data[0], list):
                            if len(raw_data) == device.channel_count:
                                raw_data = [[val] for val in raw_data]
                            elif len(raw_data) % device.channel_count == 0:
                                raw_data = [raw_data[i::device.channel_count] for i in range(device.channel_count)]
                            else:
                                vals_per_chan = len(raw_data) // device.channel_count + 1
                                raw_data = [raw_data[i:i+vals_per_chan] if i < len(raw_data) else [0.0] for i in range(0, device.channel_count)]
                    else:
                        raw_data = [[0.0] for _ in range(device.channel_count)]

                    while len(raw_data) < device.channel_count:
                        raw_data.append([0.0])
                    raw_data = raw_data[:device.channel_count]

                    # Tare command check (apply targeted or legacy to all)
                    try:
                        device_type = str(device.device_info.get('device_type', '')).lower()
                        should_tare = False
                        if do_tare_all:
                            should_tare = True
                        elif 'lc' in device_type and do_tare_lc:
                            should_tare = True
                        elif ('pt' in device_type or 'pressure' in device_type) and do_tare_pt:
                            should_tare = True
                        if should_tare and hasattr(device, 'tare'):
                            device.tare(raw_data)
                            logger.info(f"Tare executed for {device.device_info['device_type']}")
                    except Exception as e:
                        logger.error(f"Failed to execute tare: {e}")

                    processed_data = device.process_data(raw_data)
                    processed_data["timestamp"] = time.time()
                    processed_data["source"] = device.device_info.get('device_type')

                    # Snapshot latest channels for merging
                    channels = processed_data.get('channels', [])
                    if channels:
                        first = channels[0]
                        if 'pressure_psi' in first:
                            self._last_pt_channels = channels
                        elif 'v_per_v' in first or 'lbf' in first:
                            self._last_lc_channels = channels

                    # Also send per-device frame for backward compatibility
                    try:
                        self.send_to_node_sync(processed_data)
                    except Exception as e:
                        logger.error(f"Failed to send per-device frame: {e}")

                except Exception as e:
                    logger.error(f"Error during acquisition on {device.device_info['device_type']} ({device.module_name}): {e}")

            # After processing all devices, clear tare command files if present
            try:
                if do_tare_all and self.tare_cmd_file.exists():
                    self.tare_cmd_file.unlink()
                if do_tare_lc and self.tare_lc_cmd_file.exists():
                    self.tare_lc_cmd_file.unlink()
                if do_tare_pt and self.tare_pt_cmd_file.exists():
                    self.tare_pt_cmd_file.unlink()
            except FileNotFoundError:
                pass
            except Exception:
                pass

            # Emit merged frame to Node at 100 Hz cadence
            merged = {
                'timestamp': time.time(),
                'source': 'Merged',
                'channels': []
            }
            if isinstance(self._last_pt_channels, list):
                merged['channels'].extend(self._last_pt_channels)
            if isinstance(self._last_lc_channels, list):
                merged['channels'].extend(self._last_lc_channels)

            try:
                self.send_to_node_sync(merged)
            except Exception as e:
                logger.error(f"Failed to send merged frame: {e}")

            # Log merged CSV row if enabled
            if self.logging_enabled:
                try:
                    self._write_merged_log_entry()
                except Exception as e:
                    logger.error(f"Failed to write merged log entry: {e}")

            # Pace loop to ~100 Hz
            elapsed = time.time() - loop_t0
            remaining = target_period_s - elapsed
            if remaining > 0:
                time.sleep(remaining)

        # Stop and clear tasks on exit
        for _, task in self._device_tasks:
            try:
                task.stop()
            except Exception:
                pass
            try:
                task.close()
            except Exception:
                pass
        self._device_tasks = []
    
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
            
            # Write merged header: PT psi + PT mA + LC lbf + LC V/V
            header = ['timestamp', 'elapsed_ms']
            for i in range(16):
                header.append(f'PT{i}_psi')
            for i in range(16):
                header.append(f'PT{i}_mA')
            for i in range(4):
                header.append(f'LC{i}_lbf')
            for i in range(4):
                header.append(f'LC{i}_VperV')
            
            self.log_writer.writerow(header)
            self.log_file.flush()
            
            self.logging_enabled = True
            self.log_filename = str(log_path)
            self.log_data_count = 0
            self.log_start_time = time.time()
            
            logger.info(f"Started CSV logging: {self.log_filename}")
            try:
                self._write_logging_status_file(active=True)
            except Exception:
                pass
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
            
            try:
                self._write_logging_status_file(active=False, filename=filename, rows=rows)
            except Exception:
                pass

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
    
    def _write_merged_log_entry(self) -> None:
        """Write a merged PT+LC row to CSV log using last-known values."""
        if not self.logging_enabled or self.log_writer is None:
            return

        with self.log_lock:
            # Use UTC ISO format with Z suffix for clarity
            timestamp = datetime.utcnow().isoformat() + 'Z'
            elapsed_ms = int((time.time() - self.log_start_time) * 1000) if self.log_start_time else 0

            ptPsi = [''] * 16
            ptmA = [''] * 16
            lcLbf = [''] * 4
            lcVv = [''] * 4

            # Fill PT arrays
            for ch in (self._last_pt_channels or []):
                try:
                    idx = int(ch.get('channel', -1))
                except Exception:
                    idx = -1
                if 0 <= idx < 16:
                    if 'pressure_psi' in ch:
                        ptPsi[idx] = ch.get('pressure_psi', '')
                    if 'current_ma' in ch:
                        ptmA[idx] = ch.get('current_ma', '')

            # Fill LC arrays
            for ch in (self._last_lc_channels or []):
                try:
                    idx = int(ch.get('channel', -1))
                except Exception:
                    idx = -1
                if 0 <= idx < 4:
                    if 'lbf' in ch:
                        lcLbf[idx] = ch.get('lbf', '')
                    if 'v_per_v' in ch:
                        lcVv[idx] = ch.get('v_per_v', '')

            row = [timestamp, elapsed_ms]
            row.extend(ptPsi)
            row.extend(ptmA)
            row.extend(lcLbf)
            row.extend(lcVv)

            self.log_writer.writerow(row)
            self.log_data_count += 1

            # Flush every 10 rows to ensure data is written
            if self.log_data_count % 10 == 0:
                self.log_file.flush()
                try:
                    self._write_logging_status_file(active=True)
                except Exception:
                    pass

    def _write_logging_status_file(self, active: bool, filename: str = None, rows: int = None) -> None:
        """Persist logging status for Node/UI to read."""
        try:
            data = {
                "active": active,
                "filename": filename if filename is not None else self.log_filename,
                "rows": rows if rows is not None else self.log_data_count,
                "elapsed_sec": (time.time() - self.log_start_time) if (active and self.log_start_time) else 0,
                "updated_at": datetime.now().isoformat(),
            }
            with open(self.status_file, 'w') as f:
                json.dump(data, f)
        except Exception:
            pass
    
    def send_to_node_sync(self, data: Dict[str, Any]) -> None:
        """Send JSON data to Node.js TCP port (synchronous)."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(0.05)
            sock.connect((self.node_host, self.node_port))
            message = json.dumps(data) + '\n'
            sock.sendall(message.encode('utf-8'))
    
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