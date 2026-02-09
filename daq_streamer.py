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
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import AsyncGenerator, Dict, Any, List, Tuple, Optional
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
    TC_MODULE_SLOT,
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
        self.pt_log_file = None
        self.pt_log_writer = None
        self.log_filename = None
        self.lc_log_file = None
        self.lc_log_writer = None
        self.lc_log_filename = None
        self.tc_log_file = None
        self.tc_log_writer = None
        self.tc_log_filename = None
        # Raw CSV logging (per-read raw samples)
        self.raw_log_file = None
        self.raw_log_writer = None
        self.raw_log_filename = None
        self.log_lock = threading.Lock()
        self.log_data_count = 0
        self.lc_log_data_count = 0
        self.tc_log_data_count = 0
        self.log_start_time = None

        # Shutdown control
        self.shutdown_cmd = Path(__file__).parent / 'shutdown_daq.cmd'
        
        # Last-known per-device channel snapshots for merged logging/UI
        self._last_pt_channels = []
        self._last_lc_channels = []
        self._last_tc_channels = []
        self.raw_snapshots: Dict[str, Any] = {}
        
        # Background thread for slow on-demand devices (TC)
        self._tc_thread = None
        self._tc_thread_stop = threading.Event()
        self._tc_data_lock = threading.Lock()
        self._tc_cached_raw = None
        self._tc_cached_processed = None

        # Persistent NI-DAQmx tasks (device -> task)
        self._device_tasks = []

        # Combined-task mode (single hardware-timed task for all channels)
        self._combined_task = None  # type: ignore[assignment]
        self._combined_slices: List[Tuple[object, slice]] = []
        self.sample_clock_hz: float = 100.0
        self.samples_per_read: int = 10
        self._accumulated_pt_samples: int = 0
        self._accumulated_lc_samples: int = 0
        self._accumulated_tc_samples: int = 0
        self.loop_period_s: float = 0.10
        self.read_timeout_s: float = 1.0
        self._pending_pt_rows: List[Dict[str, Any]] = []
        self._pending_lc_rows: List[Dict[str, Any]] = []
        self._pending_tc_rows: List[Dict[str, Any]] = []
        self._loop_index: int = 0
        self._next_loop_deadline: Optional[float] = None
        self._last_loop_period: float = self.loop_period_s
        self._loop_rate_probe_t0: Optional[float] = None
        self._loop_rate_probe_index: Optional[int] = None
        # Watchdogs and last-knowns
        self._device_miss: Dict[int, int] = {}
        self._last_lc_row_lbf: List[Any] = [''] * 4
        self._last_lc_row_vv: List[Any] = [''] * 4

        # Disable hardware sync: NI-9237 (simultaneous delta-sigma) and NI-9208 (multiplexed scanning)
        # use incompatible ADC architectures that cannot share sample clocks directly.
        # Per NI literature: these must run as independent tasks with their own timebases.
        # Synchronization is achieved through host-side timestamping at 100 Hz.
        self.hw_sync_enabled: bool = False

        # Only send merged frames to Node by default; per-device frames are optional
        self.send_per_device: bool = False

        # Prefer per-device tasks by default; combined can be flaky across modules
        self.use_combined_task: bool = False

        # Logging status file for UI/Node to read
        self.status_file = Path(__file__).parent / 'logging_status.json'
        
        # Performance monitoring (from debug script lessons)
        self._last_sample_values: Dict[str, List[float]] = {}  # Track last values per device
        self._stale_count: int = 0  # Count of stale reads
        self._total_reads: int = 0  # Total reads for validation
        self._read_latencies: List[float] = []  # Track read latencies
        self._max_latency_history: int = 1000  # Keep last N latencies
        self._task_restart_count: Dict[str, int] = {}  # Track restarts per device
        self._last_restart_time: Dict[str, float] = {}  # Track restart timing
        self._pending_task_restarts: Dict[int, Dict[str, Any]] = {}
        self._device_sample_rates: Dict[str, float] = {}

        # Persistent TCP socket for Node communication
        self._node_socket: socket.socket | None = None
        self._socket_lock = threading.Lock()
        self._reconnect_lock = threading.Lock()
        self._reconnecting: bool = False
        
        # Tare command file
        self.tare_cmd_file = Path(__file__).parent / 'tare.cmd'  # legacy: tare all
        self.tare_lc_cmd_file = Path(__file__).parent / 'tare_lc.cmd'
        self.tare_pt_cmd_file = Path(__file__).parent / 'tare_pt.cmd'
        self.tare_config_file = Path(__file__).parent / 'tare_config.json'
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
                elif device_name == "tc_card":
                    slot = TC_MODULE_SLOT

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
                
                # Perform bridge offset nulling calibration on LC before starting
                # Per NI literature: removes initial zero offset using internal DAC
                if hasattr(lc_dev, 'perform_bridge_offset_nulling') and not lc_dev.offset_nulled:
                    try:
                        lc_dev.perform_bridge_offset_nulling(lc_task)
                    except Exception as e:
                        logger.warning(f"Bridge offset nulling skipped: {e}")
                
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
                        if ('-201401' in msg or 'Make sure the device is connected' in msg) and attempt < 2:
                            # Network chassis not ready yet; allow additional settle time
                            logger.warning(
                                f"{device.device_info['device_type']} not reachable (attempt {attempt+1}); "
                                "waiting 5s for chassis to come online"
                            )
                            time.sleep(5.0)
                            continue
                        logger.error(f"Failed to start device {device.device_info['device_type']}: {e}")
                        break
                if not started:
                    continue

            # Derive loop timing from the configured device rates (exclude slow on-demand devices)
            try:
                rates = [float(getattr(d, 'sample_rate', 0.0)) for d, _ in self._device_tasks
                         if not getattr(d, 'on_demand', False)]
                rates = [r for r in rates if r > 0.0]
                if rates:
                    self.sample_clock_hz = min(rates)
            except Exception:
                pass
            try:
                chunk_sizes = [int(getattr(d, 'samples_per_channel', 0)) for d, _ in self._device_tasks
                               if not getattr(d, 'on_demand', False)]
                chunk_sizes = [c for c in chunk_sizes if c > 0]
                if chunk_sizes:
                    self.samples_per_read = min(chunk_sizes)
            except Exception:
                pass
            self._recompute_loop_timing()
            
            # Start background thread for on-demand (slow) devices like TC
            self._start_on_demand_threads()

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
        
        # Performance monitoring
        last_perf_report = time.perf_counter()
        perf_report_interval = 60.0  # Report every 60 seconds
        while self.running:
            loop_t0 = time.perf_counter()
            self._loop_index += 1
            self._process_pending_restarts()
            current_period = max(0.001, self.loop_period_s)
            if abs(current_period - self._last_loop_period) > 1e-6:
                self._last_loop_period = current_period
                self._next_loop_deadline = None
            
            # Periodic performance report (from debug script lessons)
            now_perf = loop_t0
            if now_perf - last_perf_report >= perf_report_interval:
                if self._read_latencies:
                    avg_latency = sum(self._read_latencies) / len(self._read_latencies)
                    max_latency = max(self._read_latencies)
                    stale_pct = (self._stale_count / max(1, self._total_reads)) * 100
                    logger.info(
                        f"Performance: avg_latency={avg_latency*1000:.2f}ms, max_latency={max_latency*1000:.2f}ms, "
                        f"stale={self._stale_count}/{self._total_reads} ({stale_pct:.1f}%), loop_rate={self.sample_clock_hz}Hz"
                    )
                last_perf_report = now_perf
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
            targeted_pt_channels, targeted_lc_channels = self._collect_channel_tare_requests()
            tare_snapshot_needed = False

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

                        # Tare command per device type (legacy + targeted)
                        try:
                            device_type = str(device.device_info.get('device_type', '')).lower()
                            is_lc_device = 'lc' in device_type or 'load' in device_type
                            is_pt_device = ('pt' in device_type) or ('pressure' in device_type)
                            should_tare = False
                            if do_tare_all:
                                should_tare = True
                            elif is_lc_device and do_tare_lc:
                                should_tare = True
                            elif is_pt_device and do_tare_pt:
                                should_tare = True

                            targeted_channels: List[int] = []
                            if is_pt_device and targeted_pt_channels:
                                targeted_channels = [ch for ch in targeted_pt_channels if 0 <= ch < len(dev_raw)]
                            elif is_lc_device and targeted_lc_channels:
                                targeted_channels = [ch for ch in targeted_lc_channels if 0 <= ch < len(dev_raw)]

                            if should_tare and hasattr(device, 'tare'):
                                device.tare(dev_raw)
                                tare_snapshot_needed = True
                                logger.info(f"Tare executed for {device.device_info['device_type']}")

                            if targeted_channels:
                                if hasattr(device, 'tare_channels'):
                                    device.tare_channels(targeted_channels, dev_raw)
                                    tare_snapshot_needed = True
                                    if is_pt_device:
                                        targeted_pt_channels.difference_update(targeted_channels)
                                    elif is_lc_device:
                                        targeted_lc_channels.difference_update(targeted_channels)
                                else:
                                    logger.warning(
                                        f"Device {device.device_info.get('device_type')} does not support per-channel tare; skipping targeted request"
                                    )
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
                            elif 'temp_f' in first:
                                self._last_tc_channels = channels

                        device_key = self._device_key(device)
                        if device_key:
                            samples = processed_data.get('samples')
                            if isinstance(samples, list):
                                device_samples_map[device_key] = samples
                                try:
                                    rate = float(getattr(device, 'sample_rate', self.sample_clock_hz))
                                except Exception:
                                    rate = float(self.sample_clock_hz)
                                if rate <= 0.0:
                                    rate = float(self.sample_clock_hz) if self.sample_clock_hz > 0 else 100.0
                                self._device_sample_rates[device_key] = rate

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
                        
                        # Check if device uses on-demand reads (e.g., NI-9211 TC card)
                        is_on_demand = bool(getattr(device, 'on_demand', False))
                        
                        # On-demand devices are read by background thread - use cached data
                        if is_on_demand:
                            with self._tc_data_lock:
                                if self._tc_cached_processed:
                                    processed_data = self._tc_cached_processed.copy()
                                    processed_data["timestamp"] = time.time()
                                    processed_data["source"] = device.device_info.get('device_type')
                                    
                                    samples = processed_data.get('samples')
                                    if isinstance(samples, list):
                                        device_samples_map[dev_key] = samples
                                        self._device_sample_rates[dev_key] = 1.0  # TC at ~1 Hz effective
                            continue  # Skip to next device

                        default_chunk = max(1, int(self.samples_per_read))
                        device_chunk = int(getattr(device, 'samples_per_channel', default_chunk))
                        use_read_all = bool(getattr(device, 'read_all_available', False))

                        if use_read_all:
                            read_param = nidaqmx.constants.READ_ALL_AVAILABLE
                        else:
                            read_count = max(1, device_chunk)
                            read_param = read_count
                            # Always read the most recent fixed-size window
                            try:
                                task.in_stream.read_relative_to = nidaqmx.constants.ReadRelativeTo.MOST_RECENT_SAMPLE
                                task.in_stream.offset = -read_count
                            except Exception:
                                pass
                        
                        # Measure read latency (from debug script lessons)
                        read_t0 = time.perf_counter()
                        try:
                            raw_data = task.read(
                                number_of_samples_per_channel=read_param,
                                timeout=self.read_timeout_s if not is_on_demand else 5.0,  # Longer timeout for slow TC
                            )
                            read_latency = time.perf_counter() - read_t0
                            self._read_latencies.append(read_latency)
                            if len(self._read_latencies) > self._max_latency_history:
                                self._read_latencies.pop(0)
                            # Alert on high latency (>100 ms for 100 Hz system)
                            if read_latency > 0.100:
                                logger.warning(f"High read latency: {read_latency*1000:.1f} ms for {dev_key}")
                        except Exception as e:
                            msg = str(e)
                            # On-demand devices shouldn't hit timing errors, re-raise if they do
                            if is_on_demand:
                                logger.error(f"On-demand device {dev_key} read error: {e}")
                                raise
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
                                # Buffer overrun: clear buffer and continue (don't restart task)
                                logger.warning(f"DAQ overrun on {device.device_info['device_type']} ({device.module_name}); clearing buffer")
                                try:
                                    # Read and discard all available samples to clear buffer
                                    # Note: read_relative_to is on task.in_stream, not InStream directly
                                    avail = task.in_stream.avail_samp_per_chan
                                    if avail > 0:
                                        raw_data = task.read(
                                            number_of_samples_per_channel=avail,
                                            timeout=self.read_timeout_s,
                                        )
                                        logger.info(f"Cleared {avail} buffered samples from {dev_key}")
                                        # Reset miss counter on successful clear
                                        self._device_miss[id(device)] = 0
                                    else:
                                        # No samples to clear, just continue
                                        raw_data = task.read(
                                            number_of_samples_per_channel=1,
                                            timeout=self.read_timeout_s,
                                        )
                                except Exception as clear_err:
                                    logger.error(f"Failed to clear buffer: {clear_err}")
                                    # Only restart if clearing buffer fails repeatedly
                                    dev_id = id(device)
                                    if dev_id not in self._device_miss:
                                        self._device_miss[dev_id] = 0
                                    self._device_miss[dev_id] += 1
                                    if self._device_miss[dev_id] > 5:
                                        logger.error(f"Repeated buffer clear failures; restarting task")
                                        if self._restart_device_task(device):
                                            continue
                                    continue
                            else:
                                raise

                        # Normalize on-demand single-sample reads to expected format [[val], [val], ...]
                        if is_on_demand and isinstance(raw_data, list) and raw_data:
                            # Check if it's flat [val0, val1, ...] vs nested [[val0], [val1], ...]
                            if raw_data and not isinstance(raw_data[0], list):
                                raw_data = [[val] for val in raw_data]
                        
                        # Stale data detection (from debug script lessons)
                        self._total_reads += 1
                        if isinstance(raw_data, list) and raw_data and len(raw_data) > 0:
                            # Check if all values in this read match the last read (indicating stale data)
                            if dev_key and dev_key in self._last_sample_values:
                                last_values = self._last_sample_values[dev_key]
                                current_values = [raw_data[i][0] if isinstance(raw_data[i], list) and raw_data[i] else 0.0 
                                                  for i in range(min(len(raw_data), len(last_values)))]
                                if len(current_values) == len(last_values) and all(
                                    abs(current_values[i] - last_values[i]) < 1e-12 for i in range(len(current_values))
                                ):
                                    self._stale_count += 1
                                    if self._stale_count % 10 == 1:  # Log first and every 10th
                                        logger.warning(f"Stale data detected on {dev_key} ({self._stale_count}/{self._total_reads} reads)")
                            
                            # Update last values for next comparison
                            if dev_key:
                                self._last_sample_values[dev_key] = [
                                    raw_data[i][0] if isinstance(raw_data[i], list) and raw_data[i] else 0.0 
                                    for i in range(len(raw_data))
                                ]
                        
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
                            is_lc_device = 'lc' in device_type or 'load' in device_type
                            is_pt_device = ('pt' in device_type) or ('pressure' in device_type)
                            should_tare = False
                            if do_tare_all:
                                should_tare = True
                            elif is_lc_device and do_tare_lc:
                                should_tare = True
                            elif is_pt_device and do_tare_pt:
                                should_tare = True

                            targeted_channels: List[int] = []
                            if is_pt_device and targeted_pt_channels:
                                targeted_channels = [ch for ch in targeted_pt_channels if 0 <= ch < len(raw_data)]
                            elif is_lc_device and targeted_lc_channels:
                                targeted_channels = [ch for ch in targeted_lc_channels if 0 <= ch < len(raw_data)]

                            if should_tare and hasattr(device, 'tare'):
                                device.tare(raw_data)
                                tare_snapshot_needed = True
                                logger.info(f"Tare executed for {device.device_info['device_type']}")

                            if targeted_channels:
                                if hasattr(device, 'tare_channels'):
                                    device.tare_channels(targeted_channels, raw_data)
                                    tare_snapshot_needed = True
                                    if is_pt_device:
                                        targeted_pt_channels.difference_update(targeted_channels)
                                    elif is_lc_device:
                                        targeted_lc_channels.difference_update(targeted_channels)
                                else:
                                    logger.warning(
                                        f"Device {device.device_info.get('device_type')} does not support per-channel tare; skipping targeted request"
                                    )
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
                            elif 'temp_f' in first:
                                self._last_tc_channels = channels

                        device_key = self._device_key(device)
                        if device_key:
                            samples = processed_data.get('samples')
                            if isinstance(samples, list):
                                device_samples_map[device_key] = samples
                                try:
                                    rate = float(getattr(device, 'sample_rate', self.sample_clock_hz))
                                except Exception:
                                    rate = float(self.sample_clock_hz)
                                if rate <= 0.0:
                                    rate = float(self.sample_clock_hz) if self.sample_clock_hz > 0 else 100.0
                                self._device_sample_rates[device_key] = rate
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

            if tare_snapshot_needed:
                self._write_tare_config_snapshot()

            # Emit merged frame to Node at 100 Hz cadence
            # Server expects flat structure with 'source': 'Merged' - it handles wrapping for WS clients
            merged = {
                'timestamp': time.time(),
                'source': 'Merged',
                'channels': []
            }
            if isinstance(self._last_pt_channels, list):
                merged['channels'].extend(self._last_pt_channels)
            if isinstance(self._last_lc_channels, list):
                merged['channels'].extend(self._last_lc_channels)
            # Get TC channels with proper locking (updated by background thread)
            with self._tc_data_lock:
                if isinstance(self._last_tc_channels, list) and self._last_tc_channels:
                    merged['channels'].extend(list(self._last_tc_channels))
            
            # Add performance monitoring stats
            if self._read_latencies:
                avg_latency = sum(self._read_latencies) / len(self._read_latencies)
                max_latency = max(self._read_latencies)
                stale_pct = (self._stale_count / max(1, self._total_reads)) * 100
                total_restarts = sum(self._task_restart_count.values())
                merged['performance'] = {
                    'avg_latency_ms': round(avg_latency * 1000, 2),
                    'max_latency_ms': round(max_latency * 1000, 2),
                    'stale_reads': self._stale_count,
                    'total_reads': self._total_reads,
                    'stale_percentage': round(stale_pct, 2),
                    'sample_rate_hz': self.sample_clock_hz,
                    'loop_index': self._loop_index,
                    'task_restarts': total_restarts,
                    'restart_counts': dict(self._task_restart_count)
                }

            try:
                self.send_to_node_sync(merged)
            except Exception as e:
                logger.error(f"Failed to send merged frame: {e}")

            # Flush pending CSV rows if logging is enabled
            if self.logging_enabled:
                try:
                    self._flush_pending_logs()
                except Exception as e:
                    logger.error(f"Failed to flush log entries: {e}")

            if self._loop_index % 500 == 0:
                probe_now = time.perf_counter()
                if self._loop_rate_probe_t0 is None or self._loop_rate_probe_index is None:
                    self._loop_rate_probe_t0 = probe_now
                    self._loop_rate_probe_index = self._loop_index
                else:
                    elapsed_probe = probe_now - self._loop_rate_probe_t0
                    if elapsed_probe > 0:
                        delta_loops = self._loop_index - self._loop_rate_probe_index
                        rate = delta_loops / elapsed_probe
                        target_rate = 1.0 / self.loop_period_s if self.loop_period_s > 0 else 0.0
                        logger.info(f"DAQ loop running at {rate:.2f} Hz (target {target_rate:.2f})")
                    self._loop_rate_probe_t0 = probe_now
                    self._loop_rate_probe_index = self._loop_index

            # Pace loop to ~100 Hz
            period = max(0.001, self.loop_period_s)
            now_perf = time.perf_counter()
            if self._next_loop_deadline is None:
                self._next_loop_deadline = now_perf + period
            if now_perf < self._next_loop_deadline:
                sleep_time = self._next_loop_deadline - now_perf
                if sleep_time > 0:
                    time.sleep(sleep_time)
                now_perf = time.perf_counter()
            else:
                # behind schedule; align next deadline to now
                self._next_loop_deadline = now_perf

            self._next_loop_deadline += period

            # If we somehow slipped more than one full period behind, resync completely
            if now_perf - self._next_loop_deadline > period:
                self._next_loop_deadline = now_perf + period

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
            self._accumulated_pt_samples = 0
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
            self.read_timeout_s = max(0.05, expected_period * 1.25)
        else:
            # Sensible defaults if configuration is missing or invalid
            self.loop_period_s = 0.10
            self.read_timeout_s = 0.50

    def _start_on_demand_threads(self) -> None:
        """Start background threads for slow on-demand devices (like TC card)."""
        for device, task in self._device_tasks:
            if getattr(device, 'on_demand', False):
                dev_type = device.device_info.get('device_type', 'unknown')
                if 'tc' in dev_type.lower() or 'thermocouple' in dev_type.lower():
                    self._tc_thread_stop.clear()
                    self._tc_thread = threading.Thread(
                        target=self._tc_reader_thread,
                        args=(device, task),
                        daemon=True,
                        name="TC-Reader"
                    )
                    self._tc_thread.start()
                    logger.info(f"Started background TC reader thread for {dev_type}")

    def _tc_reader_thread(self, device, task) -> None:
        """Background thread that continuously reads TC at its slow native rate."""
        logger.info("TC reader thread started")
        while not self._tc_thread_stop.is_set():
            try:
                # Read single sample from all 4 TC channels (~350ms)
                raw_data = task.read(number_of_samples_per_channel=1, timeout=5.0)
                
                # Normalize format if needed
                if isinstance(raw_data, list) and raw_data and not isinstance(raw_data[0], list):
                    raw_data = [[val] for val in raw_data]
                
                # Process data
                processed = device.process_data(raw_data)
                
                # Cache for main loop
                with self._tc_data_lock:
                    self._tc_cached_raw = raw_data
                    self._tc_cached_processed = processed
                    
                    # Update last channels for merged output
                    channels = processed.get('channels', [])
                    if channels:
                        self._last_tc_channels = channels
                
            except Exception as e:
                if not self._tc_thread_stop.is_set():
                    logger.warning(f"TC read error: {e}")
                time.sleep(0.5)  # Back off on error
        
        logger.info("TC reader thread stopped")

    def _collect_channel_tare_requests(self) -> Tuple[set[int], set[int]]:
        base_path = Path(__file__).parent
        pt_channels: set[int] = set()
        lc_channels: set[int] = set()

        def _consume(pattern: str, target_set: set[int]) -> None:
            for cmd_path in base_path.glob(pattern):
                try:
                    channel = self._extract_channel_from_cmd(cmd_path)
                    if channel is not None:
                        target_set.add(channel)
                finally:
                    try:
                        cmd_path.unlink()
                    except FileNotFoundError:
                        pass

        _consume('tare_pt_ch*.cmd', pt_channels)
        _consume('tare_lc_ch*.cmd', lc_channels)
        return pt_channels, lc_channels

    def _extract_channel_from_cmd(self, cmd_path: Path) -> Optional[int]:
        channel: Optional[int] = None
        stem = cmd_path.stem
        candidates = [stem]
        try:
            content = cmd_path.read_text().strip()
            if content:
                candidates.append(content)
        except Exception:
            pass
        for candidate in candidates:
            match = re.search(r'ch(\d+)', candidate, re.IGNORECASE)
            if not match:
                match = re.search(r'(\d+)', candidate)
            if match:
                try:
                    channel = int(match.group(1))
                    break
                except ValueError:
                    continue
        if channel is not None and channel < 0:
            channel = None
        return channel

    def _write_tare_config_snapshot(self) -> None:
        try:
            data = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "pt_offsets": {},
                "lc_offsets": {},
            }
            for device in self.devices:
                try:
                    device_type = str(device.device_info.get('device_type', '')).lower()
                except Exception:
                    device_type = ''
                offsets = getattr(device, 'tare_offsets', None)
                if not isinstance(offsets, dict):
                    continue
                if 'pt' in device_type or 'pressure' in device_type:
                    target = data["pt_offsets"]
                elif 'lc' in device_type or 'load' in device_type:
                    target = data["lc_offsets"]
                else:
                    continue
                for ch, value in offsets.items():
                    try:
                        target[str(int(ch))] = float(value)
                    except Exception:
                        continue
            tmp_path = self.tare_config_file.with_suffix('.tmp')
            tmp_path.write_text(json.dumps(data, indent=2))
            tmp_path.replace(self.tare_config_file)
        except Exception as e:
            logger.error(f"Failed to write tare config snapshot: {e}")

    def _process_pending_restarts(self) -> None:
        if not self._pending_task_restarts:
            return

        now = time.perf_counter()
        ready_ids = [
            dev_id for dev_id, meta in list(self._pending_task_restarts.items())
            if now >= meta.get('ready_at', now)
        ]
        for dev_id in ready_ids:
            meta = self._pending_task_restarts.get(dev_id)
            if not meta:
                continue
            device = meta.get('device')
            if device is None:
                self._pending_task_restarts.pop(dev_id, None)
                continue
            index = meta.get('index')
            dev_name = meta.get('dev_name', f"{device}")
            restart_count = meta.get('restart_count')
            try:
                self._start_device_task_now(device, index, dev_name, restart_count)
                self._pending_task_restarts.pop(dev_id, None)
            except Exception as restart_error:
                logger.error(f"Failed to restart task for {dev_name}: {restart_error}")
                attempts = int(meta.get('attempts', 0)) + 1
                meta['attempts'] = attempts
                meta['ready_at'] = now + min(2.0, 0.5 * (attempts + 1))

    def _start_device_task_now(
        self,
        device: Any,
        index: Optional[int],
        dev_name: str,
        restart_count: Optional[int] = None,
    ) -> None:
        new_task = nidaqmx.Task()
        device.configure_channels(new_task)
        device.configure_timing(new_task)
        new_task.start()

        insertion_index = index if isinstance(index, int) else len(self._device_tasks)
        if insertion_index < 0:
            insertion_index = 0
        if insertion_index >= len(self._device_tasks):
            self._device_tasks.append((device, new_task))
        else:
            self._device_tasks.insert(insertion_index, (device, new_task))

        self._device_miss[id(device)] = 0
        if restart_count is not None:
            logger.info(f"Restarted NI task for {dev_name} (restart #{restart_count})")
        else:
            logger.info(f"Restarted NI task for {dev_name}")

    def _restart_device_task(self, device: Any) -> bool:
        """Attempt to restart the NI-DAQmx task for a specific device.
        
        Track restart frequency to detect chronic issues.
        """
        dev_key = self._device_key(device)
        dev_name = f"{device.device_info.get('device_type', 'Unknown')} ({device.module_name})"

        now = time.perf_counter()
        restart_count = self._task_restart_count.get(dev_key, 0) + 1
        self._task_restart_count[dev_key] = restart_count

        last_restart = self._last_restart_time.get(dev_key)
        if last_restart is not None:
            elapsed = now - last_restart
            if elapsed < 60.0 and restart_count >= 5:
                logger.error(
                    f"CRITICAL: {dev_name} restarting too frequently "
                    f"({restart_count} times in {elapsed:.1f}s). "
                    f"This indicates a fundamental timing/configuration issue."
                )
            if elapsed > 60.0:
                self._task_restart_count[dev_key] = 1
                restart_count = 1
        self._last_restart_time[dev_key] = now

        target_index: Optional[int] = None
        existing_task = None
        for i, (d, task) in enumerate(list(self._device_tasks)):
            if d is device:
                target_index = i
                existing_task = task
                break

        if existing_task is None or target_index is None:
            return False

        try:
            try:
                existing_task.stop()
            except Exception:
                pass
            try:
                existing_task.close()
            except Exception:
                pass
        finally:
            try:
                self._device_tasks.pop(target_index)
            except Exception:
                pass

        backoff = 0.0
        if restart_count > 3:
            backoff = min(2.0, 0.5 * restart_count)
            logger.warning(f"Scheduling {backoff:.1f}s backoff before restarting {dev_name}")

        if backoff <= 0.0:
            try:
                self._start_device_task_now(device, target_index, dev_name, restart_count)
                return True
            except Exception as restart_error:
                logger.error(f"Failed to restart task for {dev_name}: {restart_error}")
                return False

        ready_at = time.perf_counter() + backoff
        self._pending_task_restarts[id(device)] = {
            'device': device,
            'index': target_index,
            'dev_name': dev_name,
            'restart_count': restart_count,
            'ready_at': ready_at,
            'attempts': 0,
        }
        return True

    def _device_key(self, device: Any) -> str:
        try:
            dtype = str(getattr(device, 'device_info', {}).get('device_type', '')).lower()
        except Exception:
            dtype = ''
        if 'pt' in dtype or 'pressure' in dtype:
            return 'pt'
        if 'lc' in dtype or 'load' in dtype:
            return 'lc'
        if 'tc' in dtype or 'thermocouple' in dtype:
            return 'tc'
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
            tcDegF = [math.nan] * 4
            ptOk = False
            lcOk = False
            tcOk = False

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

            tc_samples = sample_map.get('tc')
            if isinstance(tc_samples, list) and sample_idx < len(tc_samples):
                for entry in tc_samples[sample_idx]:
                    idx = entry.get('channel')
                    if isinstance(idx, int) and 0 <= idx < 4:
                        if 'temp_f' in entry:
                            tcDegF[idx] = entry['temp_f']
                            tcOk = True

            # Do not fill-forward in the CSV; we want NaN to make gaps explicit
            if ptOk:
                self._pending_pt_rows.append({
                    'ptPsi': ptPsi,
                    'ptmA': ptmA,
                    'ptOk': True,
                    'loop_index': self._loop_index,
                })
            if lcOk:
                self._pending_lc_rows.append({
                    'lcLbf': lcLbf,
                    'lcVv': lcVv,
                    'lcOk': True,
                    'loop_index': self._loop_index,
                })
            if tcOk:
                self._pending_tc_rows.append({
                    'tcDegF': tcDegF,
                    'tcOk': True,
                    'loop_index': self._loop_index,
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
            pt_log_path = log_dir / f"{time_str}_pt.csv"
            lc_log_path = log_dir / f"{time_str}_lc.csv"
            tc_log_path = log_dir / f"{time_str}_tc.csv"
            
            # If file exists, append a number
            counter = 1
            while pt_log_path.exists() or lc_log_path.exists() or tc_log_path.exists():
                pt_log_path = log_dir / f"{time_str}_pt_{counter}.csv"
                lc_log_path = log_dir / f"{time_str}_lc_{counter}.csv"
                tc_log_path = log_dir / f"{time_str}_tc_{counter}.csv"
                counter += 1
            
            self.pt_log_file = open(pt_log_path, 'w', newline='')
            self.pt_log_writer = csv.writer(self.pt_log_file)
            self.lc_log_file = open(lc_log_path, 'w', newline='')
            self.lc_log_writer = csv.writer(self.lc_log_file)
            self.tc_log_file = open(tc_log_path, 'w', newline='')
            self.tc_log_writer = csv.writer(self.tc_log_file)
            
            # Write PT header
            pt_header = ['timestamp', 'elapsed_ms', 'wall_ms', 'loop_index', 'pt_ok']
            for i in range(16):
                pt_header.append(f'PT{i}_psi')
            for i in range(16):
                pt_header.append(f'PT{i}_mA')
            self.pt_log_writer.writerow(pt_header)
            self.pt_log_file.flush()
            
            # Write LC header
            lc_header = ['timestamp', 'elapsed_ms', 'wall_ms', 'loop_index', 'lc_ok']
            for i in range(4):
                lc_header.append(f'LC{i}_lbf')
            for i in range(4):
                lc_header.append(f'LC{i}_VperV')
            self.lc_log_writer.writerow(lc_header)
            self.lc_log_file.flush()
            
            # Write TC header
            tc_header = ['timestamp', 'elapsed_ms', 'wall_ms', 'loop_index', 'tc_ok']
            for i in range(4):
                tc_header.append(f'TC{i}_degF')
            self.tc_log_writer.writerow(tc_header)
            self.tc_log_file.flush()

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
            self.log_filename = str(pt_log_path)
            self.lc_log_filename = str(lc_log_path)
            self.tc_log_filename = str(tc_log_path)
            self.raw_log_filename = str(raw_log_path)
            self.log_data_count = 0
            self.lc_log_data_count = 0
            self.tc_log_data_count = 0
            self.log_start_time = time.time()
            self._pending_pt_rows = []
            self._pending_lc_rows = []
            self._pending_tc_rows = []
            self._accumulated_pt_samples = 0
            self._accumulated_lc_samples = 0
            self._accumulated_tc_samples = 0
            self._device_sample_rates.clear()
            
            logger.info(f"Started CSV logging: {self.log_filename}")
            try:
                self._write_logging_status_file(active=True)
            except Exception:
                pass
            return {"success": True, "message": "Logging started", "filename": self.log_filename}
        
        except Exception as e:
            logger.error(f"Failed to start logging: {e}")
            if self.pt_log_file:
                self.pt_log_file.close()
                self.pt_log_file = None
            if self.lc_log_file:
                self.lc_log_file.close()
                self.lc_log_file = None
            if self.tc_log_file:
                self.tc_log_file.close()
                self.tc_log_file = None
            return {"success": False, "message": str(e)}
    
    def stop_logging(self) -> Dict:
        """Stop CSV logging"""
        if not self.logging_enabled:
            return {"success": False, "message": "Logging not active"}
        
        try:
            try:
                self._flush_pending_logs()
            except Exception as flush_error:
                logger.error(f"Failed to flush pending logs before stop: {flush_error}")

            self.logging_enabled = False
            if self.pt_log_file:
                self.pt_log_file.close()
                self.pt_log_file = None
            if self.lc_log_file:
                self.lc_log_file.close()
                self.lc_log_file = None
            if self.tc_log_file:
                self.tc_log_file.close()
                self.tc_log_file = None
            if self.raw_log_file:
                try:
                    self.raw_log_file.close()
                except Exception:
                    pass
                self.raw_log_file = None
            self._pending_pt_rows = []
            self._pending_lc_rows = []
            self._pending_tc_rows = []
            self.log_start_time = None
            
            logger.info(
                f"Stopped CSV logging. Wrote {self.log_data_count} PT rows to {self.log_filename}, "
                f"{self.lc_log_data_count} LC rows to {self.lc_log_filename}, "
                f"{self.tc_log_data_count} TC rows to {self.tc_log_filename}"
            )
            filename = self.log_filename
            raw_filename = self.raw_log_filename
            lc_filename = self.lc_log_filename
            tc_filename = self.tc_log_filename
            pt_rows = self.log_data_count
            lc_rows = self.lc_log_data_count
            tc_rows = self.tc_log_data_count
            self.log_filename = None
            self.lc_log_filename = None
            self.tc_log_filename = None
            self.raw_log_filename = None
            self.pt_log_writer = None
            self.lc_log_writer = None
            self.tc_log_writer = None
            self.raw_log_writer = None
            self.log_data_count = 0
            self.lc_log_data_count = 0
            self.tc_log_data_count = 0
            self._device_sample_rates.clear()
            
            try:
                self._write_logging_status_file(active=False, filename=filename, rows=pt_rows)
            except Exception:
                pass

            return {
                "success": True,
                "message": "Logging stopped",
                "rows": pt_rows,
                "lc_rows": lc_rows,
                "tc_rows": tc_rows,
                "filename": filename,
                "lc_filename": lc_filename,
                "tc_filename": tc_filename,
                "raw_filename": raw_filename,
            }
        
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
    
    def _compute_log_timestamp(self, sample_index: int, sample_rate: float) -> tuple[str, int]:
        if self.log_start_time is not None and sample_rate > 0.0:
            seconds = sample_index / sample_rate
            timestamp = datetime.fromtimestamp(self.log_start_time + seconds, timezone.utc).isoformat().replace('+00:00', 'Z')
            elapsed_ms = int(seconds * 1000.0)
        else:
            timestamp = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
            elapsed_ms = 0
        return timestamp, elapsed_ms

    def _flush_pending_logs(self) -> None:
        """Write queued PT and LC rows to their respective CSV logs."""
        if not self.logging_enabled:
            return

        with self.log_lock:
            pt_rate = self._device_sample_rates.get('pt', self.sample_clock_hz if self.sample_clock_hz > 0 else 100.0)
            lc_rate = self._device_sample_rates.get('lc', self.sample_clock_hz if self.sample_clock_hz > 0 else 100.0)

            while self._pending_pt_rows and self.pt_log_writer is not None:
                sample = self._pending_pt_rows.pop(0)
                timestamp, elapsed_ms = self._compute_log_timestamp(self._accumulated_pt_samples, pt_rate)
                wall_ms = int(time.time() * 1000)
                row = [
                    timestamp,
                    elapsed_ms,
                    wall_ms,
                    sample.get('loop_index', self._loop_index),
                    1 if sample.get('ptOk') else 0,
                ]
                row.extend(sample.get('ptPsi', []))
                row.extend(sample.get('ptmA', []))
                self.pt_log_writer.writerow(row)
                self.log_data_count += 1
                self._accumulated_pt_samples += 1

                if self.log_data_count % 10 == 0 and self.pt_log_file is not None:
                    self.pt_log_file.flush()
                    try:
                        self._write_logging_status_file(active=True)
                    except Exception:
                        pass

            while self._pending_lc_rows and self.lc_log_writer is not None:
                sample = self._pending_lc_rows.pop(0)
                timestamp, elapsed_ms = self._compute_log_timestamp(self._accumulated_lc_samples, lc_rate)
                wall_ms = int(time.time() * 1000)
                row = [
                    timestamp,
                    elapsed_ms,
                    wall_ms,
                    sample.get('loop_index', self._loop_index),
                    1 if sample.get('lcOk') else 0,
                ]
                row.extend(sample.get('lcLbf', []))
                row.extend(sample.get('lcVv', []))
                self.lc_log_writer.writerow(row)
                self.lc_log_data_count += 1
                self._accumulated_lc_samples += 1

                if self.lc_log_data_count % 10 == 0 and self.lc_log_file is not None:
                    self.lc_log_file.flush()
                    try:
                        self._write_logging_status_file(active=True)
                    except Exception:
                        pass

            # TC is much slower rate, use same approach
            tc_rate = self._device_sample_rates.get('tc', 1.0)  # Default 1 Hz for slow TC
            while self._pending_tc_rows and self.tc_log_writer is not None:
                sample = self._pending_tc_rows.pop(0)
                timestamp, elapsed_ms = self._compute_log_timestamp(self._accumulated_tc_samples, tc_rate)
                wall_ms = int(time.time() * 1000)
                row = [
                    timestamp,
                    elapsed_ms,
                    wall_ms,
                    sample.get('loop_index', self._loop_index),
                    1 if sample.get('tcOk') else 0,
                ]
                row.extend(sample.get('tcDegF', []))
                self.tc_log_writer.writerow(row)
                self.tc_log_data_count += 1
                self._accumulated_tc_samples += 1

                if self.tc_log_data_count % 10 == 0 and self.tc_log_file is not None:
                    self.tc_log_file.flush()

    def _write_raw_samples_csv(self, device: Any, raw_data: List[List[float]]) -> None:
        if not self.logging_enabled or self.raw_log_writer is None:
            return
        # Use logging clock for consistent timestamps
        if self.log_start_time is not None and self.sample_clock_hz > 0.0:
            ts = datetime.fromtimestamp(self.log_start_time + (self._accumulated_pt_samples / self.sample_clock_hz), timezone.utc).isoformat().replace('+00:00', 'Z')
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
            pt_filename = filename if filename is not None else self.log_filename
            data = {
                "active": active,
                "filename": pt_filename,
                "pt_filename": pt_filename,
                "lc_filename": self.lc_log_filename,
                "tc_filename": self.tc_log_filename,
                "rows": rows if rows is not None else self.log_data_count,
                "pt_rows": self.log_data_count,
                "lc_rows": self.lc_log_data_count,
                "tc_rows": self.tc_log_data_count,
                "elapsed_sec": (time.time() - self.log_start_time) if (active and self.log_start_time) else 0,
                "updated_at": datetime.now().isoformat(),
            }
            with open(self.status_file, 'w') as f:
                json.dump(data, f)
        except Exception:
            pass
    
    def send_to_node_sync(self, data: Dict[str, Any]) -> None:
        """Send JSON data to Node.js TCP port using a persistent socket."""
        payload = (json.dumps(data) + '\n').encode('utf-8')

        sock = self._ensure_node_socket()
        if sock is None:
            return

        try:
            sock.sendall(payload)
        except (BlockingIOError, InterruptedError):
            # Send buffer full; drop this frame to keep acquisition loop realtime
            return
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError, OSError) as err:
            logger.warning(f"Node socket error ({err}); scheduling reconnect")
            self._close_node_socket()
            self._schedule_reconnect()

    def _ensure_node_socket(self) -> Optional[socket.socket]:
        with self._socket_lock:
            sock = self._node_socket
        if sock is None:
            self._schedule_reconnect()
        return sock

    def _close_node_socket(self) -> None:
        sock: Optional[socket.socket]
        with self._socket_lock:
            sock = self._node_socket
            self._node_socket = None
        if sock is not None:
            try:
                sock.close()
            except OSError:
                pass

    def _schedule_reconnect(self, delay: float = 0.0) -> None:
        with self._reconnect_lock:
            if self._reconnecting:
                return
            self._reconnecting = True

        def _worker(start_delay: float) -> None:
            self._reconnect_loop(start_delay)

        threading.Thread(target=_worker, args=(delay,), daemon=True).start()

    def _reconnect_loop(self, delay: float) -> None:
        if delay > 0.0:
            time.sleep(delay)

        try:
            while self.running:
                with self._socket_lock:
                    if self._node_socket is not None:
                        break
                try:
                    sock = socket.create_connection((self.node_host, self.node_port), timeout=0.5)
                    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                    sock.setblocking(False)
                    with self._socket_lock:
                        self._node_socket = sock
                    logger.info("Connected to Node server")
                    break
                except OSError:
                    time.sleep(1.0)
        finally:
            with self._reconnect_lock:
                self._reconnecting = False
    
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
        self._schedule_reconnect()
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
        
        # Stop TC background thread
        if self._tc_thread is not None:
            self._tc_thread_stop.set()
            self._tc_thread.join(timeout=2.0)
            self._tc_thread = None
        
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