#!/usr/bin/env python3
# DAQ Streamer - connects NI-DAQmx to Node.js via TCP

import asyncio
import math
import json
import socket
import sys
import time
import logging
import threading
import csv
import signal
from datetime import datetime, timezone
from pathlib import Path
from typing import AsyncGenerator, Dict, Any, List, Tuple
import nidaqmx
from nidaqmx.constants import AcquisitionType
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
    TARGET_SEND_HZ,
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
        # Raw CSV logging (per-read raw samples)
        self.raw_log_file = None
        self.raw_log_writer = None
        self.raw_log_filename = None
        self.log_lock = threading.Lock()
        self.log_data_count = 0
        self.log_start_time = None

        # Shutdown control
        self.shutdown_cmd = Path(__file__).parent / 'shutdown_daq.cmd'
        
        # Last-known per-device channel snapshots for merged logging/UI
        self._last_pt_channels = []
        self._last_lc_channels = []
        self.raw_snapshots: Dict[str, Any] = {}

        # Persistent NI-DAQmx tasks (device -> task)
        self._device_tasks = []

        # Combined-task mode (single hardware-timed task for all channels)
        self._combined_task = None  # type: ignore[assignment]
        self._combined_slices: List[Tuple[object, slice]] = []
        self.sample_clock_hz: float = 100.0
        self.samples_per_read: int = 10
        self._accumulated_samples: int = 0
        self.loop_period_s: float = 0.10
        self.read_timeout_s: float = 1.0
        self._pending_log_rows: List[Dict[str, List[Any]]] = []
        self._loop_index: int = 0
        # Watchdogs and last-knowns
        self._device_miss: Dict[int, int] = {}
        self._last_lc_row_lbf: List[Any] = [''] * 4
        self._last_lc_row_vv: List[Any] = [''] * 4

        # Disable cross-device hardware sync by default; prefer host-paced reads
        # across independent device tasks. Set to True only if explicit sync is required
        # and routing is known to be available on the chassis.
        self.hw_sync_enabled: bool = False

        # Only send merged frames to Node by default; per-device frames are optional
        self.send_per_device: bool = False

        # Prefer per-device tasks by default; combined can be flaky across modules
        self.use_combined_task: bool = False

        # Logging status file for UI/Node to read
        self.status_file = Path(__file__).parent / 'logging_status.json'

        # Persistent TCP socket for Node communication
        self._node_socket: socket.socket | None = None
        
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
        # Reset modules once at startup (avoid double chassis resets)
        try:
            import nidaqmx.system
            system = nidaqmx.system.System.local()
            # Reset each module once
            module_names = []
            for d in self.devices:
                try:
                    name = d.module_name
                    if name and name not in module_names:
                        module_names.append(name)
                except Exception:
                    pass
            for name in module_names:
                try:
                    system.devices[name].reset_device()
                    logger.info(f"Reset device {name} to clear reserved resources")
                    time.sleep(0.2)  # small stagger between module resets
                except Exception:
                    continue
            time.sleep(5.0)  # allow chassis/modules to settle after resets (network chassis)
        except Exception as e:
            logger.warning(f"Could not reset devices: {e}")

        logger.info(f"Starting DAQ devices: {[d.device_info['device_type'] for d in self.devices]}")

        # Try hardware-synchronized startup: PT as master clock/trigger, LC as slave
        try:
            pt_dev = next((d for d in self.devices if 'pt' in d.device_info['device_type'].lower()), None)
            lc_dev = next((d for d in self.devices if 'lc' in d.device_info['device_type'].lower()), None)
        except Exception:
            pt_dev = None; lc_dev = None

        self._device_tasks = []

        # Try combined-task mode first if enabled
        if self.use_combined_task:
            try:
                if self._start_combined_task():
                    logger.info("Using combined hardware-timed task for PT+LC")
                else:
                    logger.info("Combined task unavailable; falling back to per-device tasks")
            except Exception as e:
                logger.warning(f"Combined task start failed; falling back: {e}")

        if (self._combined_task is None) and self.hw_sync_enabled and pt_dev and lc_dev:
            try:
                pt_task = nidaqmx.Task()
                lc_task = nidaqmx.Task()
                # Configure channels
                pt_dev.configure_channels(pt_task)
                lc_dev.configure_channels(lc_task)
                # Configure timing (master uses its own clock)
                pt_dev.configure_timing(pt_task)
                # Slave: use master's sample clock and shared start trigger
                clock_src = f"/{pt_dev.module_name}/ai/SampleClock"
                trig_src = f"/{pt_dev.module_name}/ai/StartTrigger"
                try:
                    lc_task.timing.cfg_samp_clk_timing(
                        rate=float(getattr(pt_dev, 'sample_rate', 100.0)),
                        source=clock_src,
                        sample_mode=AcquisitionType.CONTINUOUS,
                        # Match the master's chunk size if available; default to 10 samples (100 ms @ 100 Hz)
                        samps_per_chan=int(max(1, int(getattr(pt_dev, 'samples_per_channel', 10)))),
                    )
                except Exception:
                    # Fallback to device's own timing if external route fails
                    lc_dev.configure_timing(lc_task)
                # Configure start trigger for slave
                try:
                    lc_task.triggers.start_trigger.cfg_dig_edge_start_trig(trig_src)
                except Exception:
                    pass
                try:
                    pt_task.triggers.start_trigger.disable_start_trig()
                except Exception:
                    pass
                # Start order: slave first, then master to emit the trigger
                lc_task.start()
                pt_task.start()
                self._device_tasks.append((pt_dev, pt_task))
                self._device_tasks.append((lc_dev, lc_task))
                logger.info(f"Started {pt_dev.device_info['device_type']} on {pt_dev.module_name} (master)")
                logger.info(f"Started {lc_dev.device_info['device_type']} on {lc_dev.module_name} (slave to {pt_dev.module_name})")
                # Derive loop timing from rates
                try:
                    self.sample_clock_hz = float(getattr(pt_dev, 'sample_rate', 100.0))
                    self.samples_per_read = int(max(1, int(getattr(pt_dev, 'samples_per_channel', 10))))
                except Exception:
                    pass
                self._recompute_loop_timing()
            except Exception as e:
                logger.warning(f"Hardware-sync start failed; falling back to independent tasks: {e}")
                # Ensure cleanup before fallback
                try:
                    lc_task.close()
                except Exception:
                    pass
                try:
                    pt_task.close()
                except Exception:
                    pass
                self._device_tasks = []
        
        if (self._combined_task is None) and (not self._device_tasks):
            # Create and start continuous tasks once (independent mode)
            for device in self.devices:
                started = False
                for attempt in range(3):
                    task = None
                    try:
                        task = nidaqmx.Task()
                        device.configure_channels(task)
                        device.configure_timing(task)
                        task.start()
                        self._device_tasks.append((device, task))
                        logger.info(f"Started {device.device_info['device_type']} on {device.module_name}")
                        started = True
                        break
                    except Exception as e:
                        msg = str(e)
                        # Clean up any partially created task
                        try:
                            if task is not None:
                                try:
                                    task.stop()
                                except Exception:
                                    pass
                                try:
                                    task.close()
                                except Exception:
                                    pass
                        except Exception:
                            pass
                        if ('-200324' in msg or '-50103' in msg) and attempt < 2:
                            # Resource errors: reset device/chassis and clean up all NI-DAQmx resources
                            try:
                                import nidaqmx.system
                                system = nidaqmx.system.System.local()
                                try:
                                    # First try to reset the specific device
                                    system.devices[device.module_name].reset_device()
                                    logger.info(f"Reset device {device.module_name} after resource error; cleaning up...")
                                except Exception:
                                    # Fallback to chassis reset
                                    system.devices[device.chassis].reset_device()
                                    logger.info(f"Reset chassis {device.chassis} after resource error; cleaning up...")

                                # Force cleanup of any lingering tasks
                                for task_name in system.tasks.task_names:
                                    try:
                                        task = nidaqmx.Task(task_name)
                                        task.stop()
                                        task.close()
                                    except Exception:
                                        pass

                                time.sleep(3.0)  # Longer backoff for resource cleanup
                            except Exception:
                                time.sleep(3.0)
                            continue
                        if ('-201003' in msg) and attempt < 2:
                            # Device cannot be accessed; allow network chassis to settle and retry
                            time.sleep(3.0)
                            continue
                        logger.error(f"Failed to start device {device.device_info['device_type']}: {e}")
                        break
                if not started:
                    continue

            # Derive loop timing from the configured device rates so logging cadence is realistic
            try:
                rates = [float(getattr(d, 'sample_rate', 0.0)) for d, _ in self._device_tasks]
                rates = [r for r in rates if r > 0.0]
                if rates:
                    self.sample_clock_hz = min(rates)
            except Exception:
                pass
            try:
                chunk_sizes = [int(getattr(d, 'samples_per_channel', 0)) for d, _ in self._device_tasks]
                chunk_sizes = [c for c in chunk_sizes if c > 0]
                if chunk_sizes:
                    self.samples_per_read = min(chunk_sizes)
            except Exception:
                pass
            self._recompute_loop_timing()

        # Back off to configurable host cadence using TARGET_SEND_HZ
        try:
            if self.sample_clock_hz > 0:
                send_hz = max(1.0, float(TARGET_SEND_HZ))
                target_read = max(1, int(round(self.sample_clock_hz / send_hz)))
                try:
                    device_hints = [int(getattr(d, 'samples_per_channel', target_read)) for d, _ in self._device_tasks]
                    device_hints = [h for h in device_hints if h > 0]
                    if device_hints:
                        target_read = max(1, min(target_read, min(device_hints)))
                except Exception:
                    pass
                if target_read != self.samples_per_read:
                    self.samples_per_read = target_read
                    self._recompute_loop_timing()
                logger.info(
                    f"Host pacing set to ~{send_hz:.1f} Hz: rate={self.sample_clock_hz:.1f} Hz, samples_per_read={self.samples_per_read}, loop_period={self.loop_period_s:.3f}s"
                )
        except Exception:
            pass

        per_device_read_count = {id(d): 0 for d, _ in self._device_tasks}

        target_period_s = self.loop_period_s
        while self.running:
            loop_t0 = time.time()
            self._loop_index += 1
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

            # Handle shutdown request
            try:
                if self.shutdown_cmd.exists():
                    logger.info("Shutdown command detected, stopping DAQ streamer...")
                    self.running = False
                    try:
                        self.shutdown_cmd.unlink()
                    except FileNotFoundError:
                        pass
                    break
            except Exception as e:
                logger.error(f"Failed to process shutdown cmd: {e}")

            # Snapshot tare requests at top of cycle so we can apply across all devices
            do_tare_all = self.tare_cmd_file.exists()
            do_tare_lc = self.tare_lc_cmd_file.exists()
            do_tare_pt = self.tare_pt_cmd_file.exists()

            device_samples_map: Dict[str, List[List[Dict[str, Any]]]] = {}

            if self._combined_task is not None:
                try:
                    try:
                        if int(self.samples_per_read) <= 1:
                            self._combined_task.in_stream.read_relative_to = nidaqmx.constants.ReadRelativeTo.CURRENT_READ_POSITION
                            self._combined_task.in_stream.offset = 0
                        else:
                            self._combined_task.in_stream.read_relative_to = nidaqmx.constants.ReadRelativeTo.MOST_RECENT_SAMPLE
                            self._combined_task.in_stream.offset = -int(self.samples_per_read)
                    except Exception:
                        pass
                    raw_all = self._combined_task.read(
                        number_of_samples_per_channel=int(self.samples_per_read),
                        timeout=self.read_timeout_s,
                    )
                except Exception as e:
                    msg = str(e)
                    if ('-200277' in msg) or ('Invalid combination of position and offset' in msg):
                        try:
                            self._combined_task.in_stream.read_relative_to = nidaqmx.constants.ReadRelativeTo.CURRENT_READ_POSITION
                            self._combined_task.in_stream.offset = 0
                        except Exception:
                            pass
                        try:
                            raw_all = self._combined_task.read(
                                nidaqmx.constants.READ_ALL_AVAILABLE,
                                timeout=self.read_timeout_s,
                            )
                        except Exception:
                            raw_all = []  # non-fatal: proceed with empty read
                    elif ('-200279' in msg) or ('keep up with the hardware' in msg):
                        try:
                            self._combined_task.in_stream.read_relative_to = nidaqmx.constants.ReadRelativeTo.CURRENT_READ_POSITION
                            self._combined_task.in_stream.offset = 0
                        except Exception:
                            pass
                        try:
                            raw_all = self._combined_task.read(
                                number_of_samples_per_channel=1,
                                timeout=self.read_timeout_s,
                            )
                        except Exception:
                            raw_all = []  # non-fatal: proceed with empty read
                    else:
                        raise

                    # Defensive normalization to list-of-list per channel
                    try:
                        total_channels = sum(getattr(d, 'channel_count', 0) for d in self.devices)
                    except Exception:
                        total_channels = 0

                    if isinstance(raw_all, (int, float)):
                        raw_all = [[raw_all] for _ in range(max(1, total_channels))]
                    elif isinstance(raw_all, list) and raw_all and not isinstance(raw_all[0], list):
                        # Single sample per channel -> wrap
                        raw_all = [[v] for v in raw_all]
                    elif isinstance(raw_all, list):
                        # Ensure correct channel count length
                        if total_channels > 0:
                            if len(raw_all) < total_channels:
                                raw_all = raw_all + ([[]] * (total_channels - len(raw_all)))
                            elif len(raw_all) > total_channels:
                                raw_all = raw_all[:total_channels]
                    else:
                        raw_all = [[0.0]] * max(1, total_channels)

                    # Per-device slice processing
                    for device, ch_slice in self._combined_slices:
                        try:
                            dev_channels = raw_all[ch_slice] if isinstance(raw_all, list) else []
                        except Exception:
                            dev_channels = []
                        # Normalize to list-of-lists per device channel
                        dev_raw: list[list[float]] = []
                        ch_count = int(getattr(device, 'channel_count', 0))
                        for ch in range(max(0, ch_count)):
                            try:
                                ch_data = dev_channels[ch] if (isinstance(dev_channels, list) and ch < len(dev_channels)) else []
                                if isinstance(ch_data, list):
                                    dev_raw.append(ch_data)
                                elif ch_data is None:
                                    dev_raw.append([])
                                else:
                                    dev_raw.append([float(ch_data)])
                            except Exception:
                                dev_raw.append([])

                        # Optional summaries occasionally
                        if DEBUG_ENABLE and DEBUG_RAW_SUMMARY:
                            count = per_device_read_count.get(id(device), 0) + 1
                            per_device_read_count[id(device)] = count
                            if count % max(1, DEBUG_SAMPLE_EVERY_N) == 0:
                                max_channels = min(8, len(dev_raw))
                                for ch in range(max_channels):
                                    samples = dev_raw[ch]
                                    if not samples:
                                        continue
                                    avg_a = sum(samples) / len(samples)
                                    logger.info(
                                        f"{device.device_info['device_type']} ch{ch:02d} avg={avg_a:.6f} A  mA={(avg_a*1000):.3f}  n={len(samples)}"
                                    )

                        # Tare command per device type
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
                                device.tare(dev_raw)
                                logger.info(f"Tare executed for {device.device_info['device_type']}")
                        except Exception as e:
                            logger.error(f"Failed to execute tare: {e}")

                        processed_data = device.process_data(dev_raw)
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

                        device_key = self._device_key(device)
                        if device_key:
                            samples = processed_data.get('samples')
                            if isinstance(samples, list):
                                device_samples_map[device_key] = samples

                            # Optional: send per-device frames
                            if self.send_per_device:
                                try:
                                    self.send_to_node_sync(processed_data)
                                except Exception as e:
                                    logger.error(f"Failed to send per-device frame: {e}")

                except Exception as e:
                    logger.error(f"Error during combined acquisition: {e}")
            else:
                # Legacy path: iterate per-device tasks with fixed-size, most-recent reads
                for device, task in list(self._device_tasks):
                    try:
                        dev_key = self._device_key(device)
                        read_count = max(1, int(self.samples_per_read))
                        # Always read the most recent fixed-size window
                        try:
                            task.in_stream.read_relative_to = nidaqmx.constants.ReadRelativeTo.MOST_RECENT_SAMPLE
                            task.in_stream.offset = -read_count
                        except Exception:
                            pass
                        try:
                            raw_data = task.read(
                                number_of_samples_per_channel=read_count,
                                timeout=self.read_timeout_s,
                            )
                        except Exception as e:
                            msg = str(e)
                            if ('-200277' in msg) or ('Invalid combination of position and offset' in msg):
                                # Fallback to current read position, single sample
                                try:
                                    task.in_stream.read_relative_to = nidaqmx.constants.ReadRelativeTo.CURRENT_READ_POSITION
                                    task.in_stream.offset = 0
                                except Exception:
                                    pass
                                raw_data = task.read(
                                    number_of_samples_per_channel=1,
                                    timeout=self.read_timeout_s,
                                )
                            elif ('-200279' in msg) or ('keep up with the hardware' in msg):
                                logger.warning(f"DAQ overrun on {device.device_info['device_type']} ({device.module_name}); restarting task")
                                if self._restart_device_task(device):
                                    continue
                                try:
                                    task.in_stream.read_relative_to = nidaqmx.constants.ReadRelativeTo.CURRENT_READ_POSITION
                                    task.in_stream.offset = 0
                                except Exception:
                                    pass
                                try:
                                    raw_data = task.read(
                                        number_of_samples_per_channel=1,
                                        timeout=self.read_timeout_s,
                                    )
                                except Exception:
                                    continue
                            else:
                                raise

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

                        # Final coercion: ensure list-of-lists per channel
                        coerced: list[list[float]] = []
                        if isinstance(raw_data, list):
                            for ch in range(device.channel_count):
                                try:
                                    ch_data = raw_data[ch]
                                    if isinstance(ch_data, list):
                                        coerced.append(ch_data)
                                    else:
                                        coerced.append([float(ch_data)])
                                except Exception:
                                    coerced.append([0.0])
                        else:
                            coerced = [[0.0] for _ in range(device.channel_count)]
                        raw_data = coerced

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

                        # Track misses for LC (simple increment if empty data)
                        try:
                            dev_id = id(device)
                            got_any = any(len(ch) > 0 for ch in raw_data) if isinstance(raw_data, list) else False
                            if dev_key == 'lc':
                                self._device_miss[dev_id] = 0 if got_any else self._device_miss.get(dev_id, 0) + 1
                            else:
                                self._device_miss[dev_id] = 0
                        except Exception:
                            pass

                        processed_data = device.process_data(raw_data)
                        processed_data["timestamp"] = time.time()
                        processed_data["source"] = device.device_info.get('device_type')

                        # Append raw samples only when explicitly enabled; avoids heavy I/O
                        if DEBUG_RAW_SAMPLES:
                            try:
                                self._write_raw_samples_csv(device, raw_data)
                            except Exception:
                                pass

                        # Snapshot latest channels for merging
                        channels = processed_data.get('channels', [])
                        if channels:
                            first = channels[0]
                            if 'pressure_psi' in first:
                                self._last_pt_channels = channels
                            elif 'v_per_v' in first or 'lbf' in first:
                                self._last_lc_channels = channels

                        device_key = self._device_key(device)
                        if device_key:
                            samples = processed_data.get('samples')
                            if isinstance(samples, list):
                                device_samples_map[device_key] = samples
                                try:
                                    raw_snapshot = processed_data.get('raw')
                                    if isinstance(raw_snapshot, list):
                                        self.raw_snapshots[device_key] = raw_snapshot
                                except Exception:
                                    pass

                        # Watchdog: if LC has missed too many consecutive reads, restart its task
                        if dev_key == 'lc':
                            try:
                                miss = self._device_miss.get(id(device), 0)
                                if miss >= 25:  # ~2.5s at 10 Hz
                                    logger.warning(f"LC task read misses={miss}; restarting {device.module_name}")
                                    try:
                                        task.stop()
                                    except Exception:
                                        pass
                                    try:
                                        task.close()
                                    except Exception:
                                        pass
                                    try:
                                        new_task = nidaqmx.Task()
                                        device.configure_channels(new_task)
                                        device.configure_timing(new_task)
                                        new_task.start()
                                        for i,(d,t) in enumerate(self._device_tasks):
                                            if d is device:
                                                self._device_tasks[i] = (d, new_task)
                                                break
                                        self._device_miss[id(device)] = 0
                                        task = new_task
                                        logger.info(f"Restarted LC task on {device.module_name}")
                                    except Exception as re:
                                        logger.error(f"Failed to restart LC task: {re}")
                            except Exception:
                                pass

                    except Exception as e:
                        logger.error(f"Error during acquisition on {device.device_info['device_type']} ({device.module_name}): {e}")

            if device_samples_map:
                try:
                    self._queue_sample_rows(device_samples_map)
                except Exception as e:
                    logger.error(f"Failed to queue samples for logging: {e}")

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

            # Log merged CSV row if enabled (cap per loop to 1 write)
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
        if self._combined_task is not None:
            try:
                self._combined_task.stop()
            except Exception:
                pass
            try:
                self._combined_task.close()
            except Exception:
                pass
            self._combined_task = None
        self._close_node_socket()

    def _start_combined_task(self) -> bool:
        """Try to start a single hardware-timed task that includes all channels.

        Returns True on success, False to fall back to legacy independent tasks.
        """
        try:
            if not self.devices:
                return False

            task = nidaqmx.Task()

            # Add channels from all devices to this single task, recording their slices
            self._combined_slices = []
            chan_offset = 0
            for device in self.devices:
                before_count = len(task.ai_channels)
                device.configure_channels(task)
                after_count = len(task.ai_channels)
                added = (after_count - before_count)
                # If a device didn't add channels as expected, infer from its channel_count
                if added <= 0:
                    added = getattr(device, 'channel_count', 0)
                self._combined_slices.append((device, slice(chan_offset, chan_offset + added)))
                chan_offset += added

            # Unify timing: choose a shared rate and buffer sizing
            try:
                rates = [float(getattr(d, 'sample_rate', 200.0)) for d in self.devices]
                self.sample_clock_hz = min([r for r in rates if r > 0.0]) if rates else 200.0
            except Exception:
                self.sample_clock_hz = 200.0

            try:
                chunk_sizes = [int(getattr(d, 'samples_per_channel', 10)) for d in self.devices]
                self.samples_per_read = max(1, min(chunk_sizes) if chunk_sizes else 10)
            except Exception:
                self.samples_per_read = 10

            task.timing.cfg_samp_clk_timing(
                rate=self.sample_clock_hz,
                sample_mode=AcquisitionType.CONTINUOUS,
                samps_per_chan=int(self.sample_clock_hz) * 20,
            )
            try:
                task.in_stream.input_buf_size = int(self.sample_clock_hz * 100)
            except Exception:
                pass
            try:
                task.in_stream.over_write = nidaqmx.constants.OverwriteMode.OVERWRITE_OLDEST
            except Exception:
                pass

            task.start()

            self._combined_task = task
            self._accumulated_samples = 0
            self._recompute_loop_timing()
            logger.info(
                f"Combined task started @ {self.sample_clock_hz:.1f} Hz, chunk {self.samples_per_read} samples, total channels {chan_offset}"
            )
            return True
        except Exception as e:
            logger.warning(f"Could not start combined task: {e}")
            # Ensure cleanup of any partially created task
            try:
                task.stop()  # type: ignore[name-defined]
            except Exception:
                pass
            try:
                task.close()  # type: ignore[name-defined]
            except Exception:
                pass
            self._combined_task = None
            self._combined_slices = []
            return False
    
    def _recompute_loop_timing(self) -> None:
        """Derive loop/timeout settings from the active sampling configuration."""
        if self.sample_clock_hz > 0 and self.samples_per_read > 0:
            expected_period = self.samples_per_read / self.sample_clock_hz
            # Ensure we do not spin faster than hardware can provide fresh data
            self.loop_period_s = max(0.001, expected_period)
            # Give generous slack for scheduling jitter
            self.read_timeout_s = max(0.20, expected_period * 3.0)
        else:
            # Sensible defaults if configuration is missing or invalid
            self.loop_period_s = 0.10
            self.read_timeout_s = 0.50

    def _restart_device_task(self, device: Any) -> bool:
        """Attempt to restart the NI-DAQmx task for a specific device."""
        for i, (d, task) in enumerate(list(self._device_tasks)):
            if d is not device:
                continue
            try:
                try:
                    task.stop()
                except Exception:
                    pass
                try:
                    task.close()
                except Exception:
                    pass
                new_task = nidaqmx.Task()
                device.configure_channels(new_task)
                device.configure_timing(new_task)
                new_task.start()
                self._device_tasks[i] = (device, new_task)
                self._device_miss[id(device)] = 0
                logger.info(f"Restarted NI task for {device.device_info['device_type']} ({device.module_name})")
                return True
            except Exception as restart_error:
                logger.error(f"Failed to restart task for {device.device_info['device_type']}: {restart_error}")
                return False
        return False

    def _device_key(self, device: Any) -> str:
        try:
            dtype = str(getattr(device, 'device_info', {}).get('device_type', '')).lower()
        except Exception:
            dtype = ''
        if 'pt' in dtype or 'pressure' in dtype:
            return 'pt'
        if 'lc' in dtype or 'load' in dtype:
            return 'lc'
        return ''

    def _queue_sample_rows(self, sample_map: Dict[str, List[List[Dict[str, Any]]]]) -> None:
        if not self.logging_enabled:
            return

        max_samples = 0
        for samples in sample_map.values():
            if isinstance(samples, list):
                max_samples = max(max_samples, len(samples))

        if max_samples == 0:
            return

        for sample_idx in range(max_samples):
            ptPsi = [math.nan] * 16
            ptmA = [math.nan] * 16
            lcLbf = [math.nan] * 4
            lcVv = [math.nan] * 4
            ptOk = False
            lcOk = False

            pt_samples = sample_map.get('pt')
            if isinstance(pt_samples, list) and sample_idx < len(pt_samples):
                for entry in pt_samples[sample_idx]:
                    idx = entry.get('channel')
                    if isinstance(idx, int) and 0 <= idx < 16:
                        if 'pressure_psi' in entry:
                            ptPsi[idx] = entry['pressure_psi']
                            ptOk = True
                        if 'current_ma' in entry:
                            ptmA[idx] = entry['current_ma']

            lc_samples = sample_map.get('lc')
            if isinstance(lc_samples, list) and sample_idx < len(lc_samples):
                for entry in lc_samples[sample_idx]:
                    idx = entry.get('channel')
                    if isinstance(idx, int) and 0 <= idx < 4:
                        if 'lbf' in entry:
                            lcLbf[idx] = entry['lbf']
                            lcOk = True
                        if 'v_per_v' in entry:
                            lcVv[idx] = entry['v_per_v']
            # Do not fill-forward in the CSV; we want NaN to make gaps explicit

            self._pending_log_rows.append({
                'ptPsi': ptPsi,
                'ptmA': ptmA,
                'lcLbf': lcLbf,
                'lcVv': lcVv,
                'ptOk': ptOk,
                'lcOk': lcOk,
            })

        # After queuing, we can write rows during logging flush
    
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
            header = ['timestamp', 'elapsed_ms', 'wall_ms', 'loop_index', 'pt_ok', 'lc_ok']
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

            # Start raw CSV alongside merged CSV
            raw_log_path = log_dir / f"{time_str}_raw.csv"
            counter_raw = 1
            while raw_log_path.exists():
                raw_log_path = log_dir / f"{time_str}_raw_{counter_raw}.csv"
                counter_raw += 1
            self.raw_log_file = open(raw_log_path, 'w', newline='')
            self.raw_log_writer = csv.writer(self.raw_log_file)
            self.raw_log_writer.writerow(['timestamp', 'device', 'channel', 'num_samples', 'values_json'])
            self.raw_log_file.flush()
            
            self.logging_enabled = True
            self.log_filename = str(log_path)
            self.raw_log_filename = str(raw_log_path)
            self.log_data_count = 0
            self.log_start_time = time.time()
            self._pending_log_rows = []
            self._accumulated_samples = 0
            
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
            if self.raw_log_file:
                try:
                    self.raw_log_file.close()
                except Exception:
                    pass
                self.raw_log_file = None
            self._pending_log_rows = []
            self.log_start_time = None
            
            logger.info(f"Stopped CSV logging. Wrote {self.log_data_count} rows to {self.log_filename}")
            filename = self.log_filename
            raw_filename = self.raw_log_filename
            rows = self.log_data_count
            self.log_filename = None
            self.raw_log_filename = None
            self.log_writer = None
            self.raw_log_writer = None
            self.log_data_count = 0
            
            try:
                self._write_logging_status_file(active=False, filename=filename, rows=rows)
            except Exception:
                pass

            return {"success": True, "message": "Logging stopped", "rows": rows, "filename": filename, "raw_filename": raw_filename}
        
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
            while self._pending_log_rows:
                sample = self._pending_log_rows.pop(0)

                if self.sample_clock_hz > 0.0 and self.log_start_time:
                    base_time = self.log_start_time + (self._accumulated_samples / self.sample_clock_hz)
                    timestamp = datetime.fromtimestamp(base_time, timezone.utc).isoformat().replace('+00:00', 'Z')
                else:
                    timestamp = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
                elapsed_ms = int((self._accumulated_samples / self.sample_clock_hz) * 1000)

                # Additional diagnostics: wall-clock ms and loop index
                wall_ms = int(time.time() * 1000)
                # Presence flags prefer explicit markers from queue, fallback to numeric check
                def _has_numeric(vals: list) -> bool:
                    try:
                        return any(isinstance(v, (int, float)) for v in vals)
                    except Exception:
                        return False
                if 'ptOk' in sample:
                    pt_ok = 1 if sample.get('ptOk') else 0
                else:
                    pt_ok = 1 if _has_numeric(sample.get('ptPsi', [])) else 0
                if 'lcOk' in sample:
                    lc_ok = 1 if sample.get('lcOk') else 0
                else:
                    lc_ok = 1 if _has_numeric(sample.get('lcLbf', [])) else 0
                row = [timestamp, elapsed_ms, wall_ms, self._loop_index, pt_ok, lc_ok]
                row.extend(sample['ptPsi'])
                row.extend(sample['ptmA'])
                row.extend(sample['lcLbf'])
                row.extend(sample['lcVv'])

                self.log_writer.writerow(row)
                self.log_data_count += 1
                # Advance by the chunk size to reflect wall-time distance between rows
                try:
                    inc = int(self.samples_per_read) if int(self.samples_per_read) > 0 else 1
                except Exception:
                    inc = 1
                self._accumulated_samples += inc

                if self.log_data_count % 10 == 0:
                    self.log_file.flush()
                    try:
                        self._write_logging_status_file(active=True)
                    except Exception:
                        pass

    def _write_raw_samples_csv(self, device: Any, raw_data: List[List[float]]) -> None:
        if not self.logging_enabled or self.raw_log_writer is None:
            return
        # Use logging clock for consistent timestamps
        if self.log_start_time is not None and self.sample_clock_hz > 0.0:
            ts = datetime.fromtimestamp(self.log_start_time + (self._accumulated_samples / self.sample_clock_hz), timezone.utc).isoformat().replace('+00:00', 'Z')
        else:
            ts = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
        dev_name = str(getattr(device, 'device_info', {}).get('device_type', 'unknown'))
        # Ensure per-channel shape
        data = raw_data if isinstance(raw_data, list) else []
        for ch_idx, samples in enumerate(data):
            try:
                vals = samples if isinstance(samples, list) else []
                self.raw_log_writer.writerow([ts, dev_name, ch_idx, len(vals), json.dumps(vals)])
            except Exception:
                continue
        try:
            self.raw_log_file.flush()
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
        """Send JSON data to Node.js TCP port using a persistent socket."""
        message = json.dumps(data) + '\n'
        payload = message.encode('utf-8')

        for attempt in range(2):
            sock = self._ensure_node_socket()
            if sock is None:
                time.sleep(0.02)
                continue
            try:
                sock.sendall(payload)
                return
            except OSError:
                self._close_node_socket()
                time.sleep(0.02)

    def _ensure_node_socket(self) -> socket.socket | None:
        if self._node_socket is not None:
            return self._node_socket
        try:
            sock = socket.create_connection((self.node_host, self.node_port), timeout=0.5)
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            sock.settimeout(0.5)
            self._node_socket = sock
            return sock
        except OSError:
            self._close_node_socket()
            return None

    def _close_node_socket(self) -> None:
        if self._node_socket is not None:
            try:
                self._node_socket.close()
            except OSError:
                pass
            self._node_socket = None
    
    def _cleanup_lingering_resources(self) -> None:
        """Clean up any lingering NI-DAQmx resources at startup"""
        try:
            import nidaqmx.system
            system = nidaqmx.system.System.local()
            logger.info("Cleaning up any lingering NI-DAQmx resources...")

            # Stop and close any existing tasks
            for task_name in system.tasks.task_names:
                try:
                    task = nidaqmx.Task(task_name)
                    task.stop()
                    task.close()
                    logger.info(f"Cleaned up lingering task: {task_name}")
                except Exception:
                    pass

            # Intentionally skip chassis resets here to avoid network settle races

        except Exception as e:
            logger.warning(f"Resource cleanup failed: {e}")

    def run(self) -> None:
        # Clean up any lingering resources before starting
        self._cleanup_lingering_resources()
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
        if self._combined_task is not None:
            try:
                self._combined_task.stop()
            except Exception:
                pass
            try:
                self._combined_task.close()
            except Exception:
                pass
            self._combined_task = None

# Global reference for signal handler
streamer_instance = None

def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    logger.info(f"Received signal {signum}, shutting down gracefully...")
    if streamer_instance is not None:
        streamer_instance.stop()
    sys.exit(0)

def main():
    global streamer_instance

    # Set up signal handlers for graceful shutdown
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    streamer_instance = DAQStreamer()

    try:
        streamer_instance.run()
    except KeyboardInterrupt:
        logger.info("Received interrupt signal")
    finally:
        streamer_instance.stop()

if __name__ == "__main__":
    main() 