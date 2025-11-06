"""NI cDAQ poller for 9208 and 9237 modules with rate validation."""

from __future__ import annotations

import argparse
import csv
import json
import logging
import math
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import numpy as np

import nidaqmx
import nidaqmx.system
from nidaqmx import stream_readers
from nidaqmx.constants import (
    AcquisitionType,
    CurrentShuntResistorLocation,
    CurrentUnits,
    ExcitationSource,
    OverwriteMode,
    ShuntCalSelect,
    ShuntCalSource,
    ShuntElementLocation,
    StrainGageBridgeType,
    StrainUnits,
    TaskMode,
)
from nidaqmx.errors import DaqError

try:
    from config import (
        ACTIVE_DEVICES,
        DEVICE_CHASSIS,
        LC_MODULE_SLOT,
        MODULE_SLOT,
        PT_MODULE_SLOT,
    )
except Exception:  # pragma: no cover - debug script should still run with defaults
    ACTIVE_DEVICES = ()
    DEVICE_CHASSIS = None
    LC_MODULE_SLOT = 1
    MODULE_SLOT = 1
    PT_MODULE_SLOT = 1


_DEFAULT_PT_CHANNEL_COUNT = 16
_DEFAULT_LC_CHANNEL_COUNT = 4

# Maximum supported channels per device variant we know about
_MAX_PT_CHANNELS = 16  # NI-9208 has 16 AI current channels
_MAX_LC_CHANNELS = 4   # NI-9237 has 4 bridge channels


def _extract_channel_index(token: str) -> Optional[int]:
    token = token.strip()
    if not token:
        return None
    for prefix in ("ai", "ci", "ao", "co"):
        if token.startswith(prefix):
            token = token[len(prefix):]
            break
    try:
        return int(token)
    except ValueError:
        return None


def _count_physical_channels(physical: str) -> int:
    if not physical:
        return 0
    total = 0
    for part in physical.split(","):
        part = part.strip()
        if not part:
            continue
        channel_spec = part.split("/")[-1] if "/" in part else part
        if ":" in channel_spec:
            start_token, end_token = channel_spec.split(":", 1)
            start_idx = _extract_channel_index(start_token)
            end_idx = _extract_channel_index(end_token)
            if start_idx is not None and end_idx is not None:
                total += abs(end_idx - start_idx) + 1
        else:
            if _extract_channel_index(channel_spec) is not None:
                total += 1
    return total


def _safe_int(*values: object, default: int) -> int:
    for value in values:
        try:
            if value is None:
                continue
            return int(value)
        except (TypeError, ValueError):
            continue
    return default


def _load_pt_channel_count() -> int:
    config_path = Path(__file__).with_name("interface_config.json")
    try:
        with config_path.open("r", encoding="utf-8") as stream:
            data = json.load(stream)
        sensors = data.get("sensors", [])
        channels = [int(sensor.get("channel")) for sensor in sensors if isinstance(sensor, dict) and "channel" in sensor]
        if channels:
            max_idx = max(channels)
            if max_idx >= 0:
                return max_idx + 1
    except Exception:
        pass
    return _DEFAULT_PT_CHANNEL_COUNT


def _load_lc_channel_count() -> int:
    config_path = Path(__file__).with_name("lc_channels.json")
    try:
        with config_path.open("r", encoding="utf-8") as stream:
            data = json.load(stream)
        channels = data.get("channels", [])
        indices: list[int] = []
        for entry in channels:
            if not isinstance(entry, dict):
                continue
            lc_info = entry.get("lc") if isinstance(entry.get("lc"), dict) else None
            if lc_info and "id" in lc_info:
                try:
                    idx = int(lc_info["id"]) - 1
                except (TypeError, ValueError):
                    continue
                indices.append(idx)
        valid_indices = [idx for idx in indices if idx >= 0]
        if valid_indices:
            max_idx = max(valid_indices)
            if max_idx >= 0:
                return max_idx + 1
    except Exception:
        pass
    return _DEFAULT_LC_CHANNEL_COUNT


def _detect_module_name(product_hint: str, preferred_slot: int) -> Optional[str]:
    try:
        system = nidaqmx.system.System.local()
    except Exception:
        system = None

    if system is not None:
        try:
            product_hint_lower = product_hint.lower()
        except AttributeError:
            product_hint_lower = str(product_hint).lower()

        devices = list(getattr(system, "devices", []))
        # Prefer exact chassis + product match
        if DEVICE_CHASSIS:
            for device in devices:
                try:
                    name = getattr(device, "name", "")
                    product_type = str(getattr(device, "product_type", "")).lower()
                except Exception:
                    continue
                if name.startswith(DEVICE_CHASSIS) and product_hint_lower in product_type:
                    return name
        # Fallback: any product match
        for device in devices:
            try:
                product_type = str(getattr(device, "product_type", "")).lower()
            except Exception:
                continue
            if product_hint_lower in product_type:
                name = getattr(device, "name", None)
                if name:
                    return name
        # Fallback: expected chassis slot if device list is available
        if DEVICE_CHASSIS:
            expected = f"{DEVICE_CHASSIS}Mod{preferred_slot}"
            for device in devices:
                if getattr(device, "name", None) == expected:
                    return expected

    if DEVICE_CHASSIS:
        return f"{DEVICE_CHASSIS}Mod{preferred_slot}"
    return None


def _auto_current_channels() -> Optional[str]:
    slot = _safe_int(PT_MODULE_SLOT, MODULE_SLOT, default=1)
    module_name = _detect_module_name("9208", slot)
    if not module_name:
        return None
    channel_count = max(1, min(_MAX_PT_CHANNELS, _load_pt_channel_count()))
    return f"{module_name}/ai0:{channel_count - 1}"


def _auto_bridge_channels() -> Optional[str]:
    slot = _safe_int(LC_MODULE_SLOT, MODULE_SLOT, default=1)
    module_name = _detect_module_name("9237", slot)
    if not module_name:
        return None
    channel_count = max(1, min(_MAX_LC_CHANNELS, _load_lc_channel_count()))
    return f"{module_name}/ai0:{channel_count - 1}"

@dataclass
class CurrentModuleConfig:
    physical_channels: Optional[str] = None
    min_val: float = -0.02
    max_val: float = 0.02
    shunt_resistor_location: CurrentShuntResistorLocation = (
        CurrentShuntResistorLocation.LET_DRIVER_CHOOSE
    )
    shunt_resistor_value: float = 0.0
    channel_count: int = 0


@dataclass
class BridgeModuleConfig:
    physical_channels: Optional[str] = None
    min_val: float = -250e-6
    max_val: float = 250e-6
    nominal_resistance: float = 350.0
    gauge_factor: float = 2.0
    strain_config: StrainGageBridgeType = StrainGageBridgeType.FULL_BRIDGE_I
    strain_units: StrainUnits = StrainUnits.STRAIN
    voltage_excit_source: ExcitationSource = ExcitationSource.INTERNAL
    voltage_excit_val: float = 2.5
    initial_bridge_voltage: float = 0.0
    poisson_ratio: float = 0.3
    lead_wire_resistance: float = 0.0
    shunt_resistor_value: float = 100_000.0
    shunt_resistor_location: ShuntElementLocation = ShuntElementLocation.R3
    shunt_resistor_select: ShuntCalSelect = ShuntCalSelect.A
    shunt_resistor_source: ShuntCalSource = ShuntCalSource.DEFAULT
    channel_count: int = 0


@dataclass
class RunConfig:
    rate_hz: int
    duration_s: float
    timeout_s: float
    buffer_seconds: float
    rate_tolerance: float
    bridge_offset_cal: bool
    bridge_shunt_cal: bool
    log_path: Optional[Path]
    display_interval_s: float
    latency_alert_s: float
    require_lc: bool
    require_pt: bool


@dataclass
class RunResult:
    elapsed_s: float
    samples_per_channel: int
    observed_rate_hz: float
    latency_mean_s: float
    latency_p95_s: float
    latency_max_s: float
    latency_std_s: float
    max_backlog_samples: int
    total_acquired_per_channel: int
    latency_alerts: int
    stale_iterations: int
    iterations: int


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate NI cDAQ acquisition rates for 9208 and 9237 modules."
    )
    parser.add_argument(
        "--rate",
        type=int,
        choices=(10, 100),
        default=100,
        help="Sample rate in Hz (default: 100).",
    )
    parser.add_argument(
        "--duration",
        type=float,
        default=300.0,
        help="Acquisition duration in seconds (default: 300).",
    )
    parser.add_argument(
        "--current-channels",
        dest="current_channels",
        type=str,
        default=None,
        help="Physical channels on the 9208 module (e.g. 'cDAQ9189-1Mod1/ai0:7').",
    )
    parser.add_argument(
        "--bridge-channels",
        dest="bridge_channels",
        type=str,
        default=None,
        help="Physical channels on the 9237 module (e.g. 'cDAQ9189-1Mod2/ai0:3').",
    )
    parser.add_argument(
        "--no-bridge-offset-cal",
        action="store_true",
        help="Skip bridge offset nulling calibration prior to the run.",
    )
    parser.add_argument(
        "--bridge-shunt-cal",
        action="store_true",
        help="Perform bridge shunt calibration prior to the run.",
    )
    parser.add_argument(
        "--bridge-gauge-factor",
        type=float,
        default=2.0,
        help="Gauge factor for bridge channels (default: 2.0).",
    )
    parser.add_argument(
        "--bridge-nominal-res",
        type=float,
        default=350.0,
        help="Nominal bridge resistance in ohms (default: 350).",
    )
    parser.add_argument(
        "--bridge-range",
        type=float,
        nargs=2,
        metavar=("MIN", "MAX"),
        default=(-250e-6, 250e-6),
        help="Bridge strain min/max in strain units (default: -250e-6 250e-6).",
    )
    parser.add_argument(
        "--current-range",
        type=float,
        nargs=2,
        metavar=("MIN", "MAX"),
        default=(-0.02, 0.02),
        help="9208 current min/max in amperes (default: -0.02 0.02).",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=None,
        help="Per-read timeout in seconds. Defaults to 1.5/Hz.",
    )
    parser.add_argument(
        "--buffer-seconds",
        type=float,
        default=5.0,
        help="Hardware buffer length in seconds per channel (default: 5).",
    )
    parser.add_argument(
        "--rate-tolerance",
        type=float,
        default=0.05,
        help="Acceptable relative rate error (default: 0.05 = 5%%).",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
        default="INFO",
        help="Logger level (default: INFO).",
    )
    parser.add_argument(
        "--log-file",
        type=Path,
        default=None,
        help="Optional CSV path to log latency/backlog metrics.",
    )
    parser.add_argument(
        "--display-interval",
        type=float,
        default=1.0,
        help="Seconds between console updates of live data (default: 1).",
    )
    parser.add_argument(
        "--latency-alert",
        type=float,
        default=0.2,
        help="Latency threshold in seconds that triggers warnings (default: 0.2).",
    )
    parser.add_argument(
        "--allow-missing-lc",
        action="store_true",
        help="Skip error if load cell channels (NI-9237) cannot be configured.",
    )
    parser.add_argument(
        "--allow-missing-pt",
        action="store_true",
        help="Skip error if current channels (NI-9208) cannot be configured.",
    )
    return parser.parse_args()


def _configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level),
        format="%(asctime)s %(levelname)s %(message)s",
    )


def _build_current_config(args: argparse.Namespace) -> CurrentModuleConfig:
    min_val, max_val = args.current_range
    physical = args.current_channels or _auto_current_channels()
    if physical:
        logging.info("Configured current channels: %s", physical)
        channel_count = _count_physical_channels(physical)
        return CurrentModuleConfig(
            physical_channels=physical,
            min_val=min_val,
            max_val=max_val,
            channel_count=channel_count,
        )
    logging.warning("No current channels configured")
    return CurrentModuleConfig(
        min_val=min_val,
        max_val=max_val,
    )


def _build_bridge_config(args: argparse.Namespace) -> BridgeModuleConfig:
    min_val, max_val = args.bridge_range
    physical = args.bridge_channels or _auto_bridge_channels()
    if physical:
        logging.info("Configured bridge channels: %s", physical)
        channel_count = _count_physical_channels(physical)
        return BridgeModuleConfig(
            physical_channels=physical,
            min_val=min_val,
            max_val=max_val,
            nominal_resistance=args.bridge_nominal_res,
            gauge_factor=args.bridge_gauge_factor,
            channel_count=channel_count,
        )
    logging.warning("No bridge channels configured")
    return BridgeModuleConfig(
        min_val=min_val,
        max_val=max_val,
        nominal_resistance=args.bridge_nominal_res,
        gauge_factor=args.bridge_gauge_factor,
    )


def _build_run_config(args: argparse.Namespace) -> RunConfig:
    timeout = args.timeout if args.timeout is not None else max(1.5 / args.rate, 0.2)
    log_path = args.log_file.expanduser() if args.log_file else None
    display_interval = max(float(args.display_interval), 0.0)
    latency_alert = max(float(args.latency_alert), 0.0)
    return RunConfig(
        rate_hz=args.rate,
        duration_s=args.duration,
        timeout_s=timeout,
        buffer_seconds=args.buffer_seconds,
        rate_tolerance=args.rate_tolerance,
        bridge_offset_cal=not args.no_bridge_offset_cal,
        bridge_shunt_cal=args.bridge_shunt_cal,
        log_path=log_path,
        display_interval_s=display_interval,
        latency_alert_s=latency_alert,
        require_lc=not args.allow_missing_lc,
        require_pt=not args.allow_missing_pt,
    )


def _configure_task(
    task: nidaqmx.Task,
    rate_hz: int,
    buffer_seconds: float,
    current_cfg: CurrentModuleConfig,
    bridge_cfg: BridgeModuleConfig,
) -> None:
    if current_cfg.physical_channels:
        logging.info("Adding current channels: %s", current_cfg.physical_channels)
        current_kwargs = {
            "physical_channel": current_cfg.physical_channels,
            "min_val": current_cfg.min_val,
            "max_val": current_cfg.max_val,
            "units": CurrentUnits.AMPS,
        }
        if current_cfg.shunt_resistor_location is not None:
            current_kwargs["shunt_resistor_loc"] = current_cfg.shunt_resistor_location
        if current_cfg.shunt_resistor_value and current_cfg.shunt_resistor_value > 0:
            current_kwargs["ext_shunt_resistor_val"] = current_cfg.shunt_resistor_value
        task.ai_channels.add_ai_current_chan(**current_kwargs)
    if bridge_cfg.physical_channels:
        logging.info("Adding bridge channels: %s", bridge_cfg.physical_channels)
        task.ai_channels.add_ai_strain_gage_chan(
            bridge_cfg.physical_channels,
            min_val=bridge_cfg.min_val,
            max_val=bridge_cfg.max_val,
            units=bridge_cfg.strain_units,
            strain_config=bridge_cfg.strain_config,
            voltage_excit_source=bridge_cfg.voltage_excit_source,
            voltage_excit_val=bridge_cfg.voltage_excit_val,
            gage_factor=bridge_cfg.gauge_factor,
            initial_bridge_voltage=bridge_cfg.initial_bridge_voltage,
            nominal_gage_resistance=bridge_cfg.nominal_resistance,
            poisson_ratio=bridge_cfg.poisson_ratio,
            lead_wire_resistance=bridge_cfg.lead_wire_resistance,
        )
    if task.number_of_channels == 0:
        raise RuntimeError("No channels configured. Provide --current-channels and/or --bridge-channels.")
    samples_per_channel = max(int(rate_hz * buffer_seconds), rate_hz * 2)
    logging.info(
        "Configuring sample clock at %d Hz with %d-sample buffer",
        rate_hz,
        samples_per_channel,
    )
    task.timing.cfg_samp_clk_timing(
        rate=rate_hz,
        active_edge=nidaqmx.constants.Edge.RISING,
        sample_mode=AcquisitionType.CONTINUOUS,
        samps_per_chan=samples_per_channel,
    )
    task.in_stream.overwrite = OverwriteMode.DO_NOT_OVERWRITE_UNREAD_SAMPLES
    task.in_stream.read_all_avail_samp = False


def _run_calibrations(
    task: nidaqmx.Task,
    bridge_cfg: BridgeModuleConfig,
    run_cfg: RunConfig,
) -> None:
    if not bridge_cfg.physical_channels:
        return
    if run_cfg.bridge_offset_cal:
        logging.info("Running bridge offset nulling on %s", bridge_cfg.physical_channels)
        try:
            task.perform_bridge_offset_nulling_cal(
                channel=bridge_cfg.physical_channels,
                skip_unsupported_channels=True,
            )
        except DaqError as exc:
            logging.warning("Bridge offset nulling failed: %s", exc)
    if run_cfg.bridge_shunt_cal:
        logging.info("Running bridge shunt calibration on %s", bridge_cfg.physical_channels)
        try:
            task.perform_bridge_shunt_cal(
                channel=bridge_cfg.physical_channels,
                shunt_resistor_value=bridge_cfg.shunt_resistor_value,
                shunt_resistor_location=bridge_cfg.shunt_resistor_location,
                shunt_resistor_select=bridge_cfg.shunt_resistor_select,
                shunt_resistor_source=bridge_cfg.shunt_resistor_source,
                bridge_resistance=bridge_cfg.nominal_resistance,
                skip_unsupported_channels=True,
            )
        except DaqError as exc:
            logging.warning("Bridge shunt calibration failed: %s", exc)


def _poll_loop(
    task: nidaqmx.Task,
    run_cfg: RunConfig,
    current_cfg: CurrentModuleConfig,
    bridge_cfg: BridgeModuleConfig,
) -> RunResult:
    reader = stream_readers.AnalogMultiChannelReader(task.in_stream)
    chunk_size = 1
    buffer = np.empty((task.number_of_channels, chunk_size), dtype=np.float64)
    latencies: list[float] = []
    samples_seen = 0
    max_backlog = 0
    start = time.perf_counter()
    last_tick = start
    next_display = start + run_cfg.display_interval_s if run_cfg.display_interval_s > 0 else math.inf
    csv_file = None
    csv_writer: Optional[csv.writer] = None
    iteration = 0
    alert_exceed_count = 0
    stale_iterations = 0
    last_total_acquired = task.in_stream.total_samp_per_chan_acquired

    current_count = current_cfg.channel_count if current_cfg.physical_channels else 0
    bridge_count = bridge_cfg.channel_count if bridge_cfg.physical_channels else 0
    total_channels = task.number_of_channels
    current_count = min(current_count, total_channels)
    bridge_count = min(bridge_count, max(0, total_channels - current_count))

    channel_labels: list[str] = []
    for idx in range(current_count):
        channel_labels.append(f"pt_ch{idx}_mean_A")
    for idx in range(bridge_count):
        channel_labels.append(f"lc_ch{idx}_mean_V_per_V")

    if run_cfg.log_path is not None:
        run_cfg.log_path.parent.mkdir(parents=True, exist_ok=True)
        csv_file = run_cfg.log_path.open("w", newline="", encoding="utf-8")
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(
            [
                "iteration",
                "timestamp_s",
                "latency_s",
                "samples_seen",
                "device_total",
                "backlog",
                "stale",
                "latency_alert",
            ]
            + channel_labels
        )

    try:
        while True:
            now = time.perf_counter()
            if now - start >= run_cfg.duration_s:
                break
            try:
                before_read = time.perf_counter()
                reader.read_many_sample(
                    buffer,
                    number_of_samples_per_channel=chunk_size,
                    timeout=run_cfg.timeout_s,
                )
            except DaqError as exc:
                raise RuntimeError(f"Read failed: {exc}") from exc
            after_read = time.perf_counter()
            iteration += 1
            latency = after_read - before_read
            latencies.append(latency)
            samples_read = chunk_size
            samples_seen += samples_read
            total_acquired = task.in_stream.total_samp_per_chan_acquired
            backlog = int(max(0, total_acquired - samples_seen))
            max_backlog = max(max_backlog, backlog)
            stale_flag = total_acquired == last_total_acquired
            if stale_flag:
                stale_iterations += 1
            last_total_acquired = total_acquired
            latency_alert_triggered = run_cfg.latency_alert_s > 0 and latency > run_cfg.latency_alert_s
            if latency_alert_triggered:
                alert_exceed_count += 1
                logging.warning(
                    "Latency %.3f ms exceeded alert threshold %.3f ms", latency * 1000.0, run_cfg.latency_alert_s * 1000.0
                )

            chunk_view = buffer[:, :samples_read]
            if samples_read:
                chunk_means = np.mean(chunk_view, axis=1)
            else:
                chunk_means = np.zeros(task.number_of_channels)

            pt_means = chunk_means[:current_count] if current_count else np.array([], dtype=float)
            lc_means = chunk_means[current_count : current_count + bridge_count] if bridge_count else np.array([], dtype=float)

            if csv_writer is not None:
                csv_writer.writerow(
                    [
                        iteration,
                        after_read - start,
                        latency,
                        samples_seen,
                        total_acquired,
                        backlog,
                        int(stale_flag),
                        int(latency_alert_triggered),
                    ]
                    + [float(val) for val in pt_means]
                    + [float(val) for val in lc_means]
                )
                if csv_file is not None and iteration % 10 == 0:
                    csv_file.flush()

            while after_read >= next_display:
                preview_current_count = min(8, pt_means.size)
                if preview_current_count:
                    preview_current = " ".join(f"{val * 1000.0:.2f}" for val in pt_means[:preview_current_count])
                else:
                    preview_current = "<none>"

                preview_lc_count = min(4, lc_means.size)
                if preview_lc_count:
                    preview_lc = " ".join(f"{val * 1000.0:.3f}" for val in lc_means[:preview_lc_count])
                else:
                    preview_lc = "<none>"

                logging.info(
                    "Live PT mA [%d]: %s | LC mV/V [%d]: %s | latency=%.3f ms backlog=%d stale=%s",
                    preview_current_count,
                    preview_current,
                    preview_lc_count,
                    preview_lc,
                    latency * 1000.0,
                    backlog,
                    "yes" if stale_flag else "no",
                )
                next_display += run_cfg.display_interval_s

            last_tick = after_read
    finally:
        if csv_file is not None:
            csv_file.close()

    elapsed = last_tick - start
    elapsed = elapsed if elapsed > 0 else run_cfg.duration_s
    observed_rate = samples_seen / elapsed if elapsed > 0 else 0.0
    lat_array = np.array(latencies, dtype=float)
    latency_mean = float(np.mean(lat_array)) if lat_array.size else 0.0
    latency_p95 = float(np.percentile(lat_array, 95)) if lat_array.size else 0.0
    latency_max = float(np.max(lat_array)) if lat_array.size else 0.0
    latency_std = float(np.std(lat_array)) if lat_array.size else 0.0
    total_acquired = task.in_stream.total_samp_per_chan_acquired
    return RunResult(
        elapsed_s=elapsed,
        samples_per_channel=samples_seen,
        observed_rate_hz=observed_rate,
        latency_mean_s=latency_mean,
        latency_p95_s=latency_p95,
        latency_max_s=latency_max,
        latency_std_s=latency_std,
        max_backlog_samples=max_backlog,
        total_acquired_per_channel=total_acquired,
        latency_alerts=alert_exceed_count,
        stale_iterations=stale_iterations,
        iterations=iteration,
    )


def _summarize(result: RunResult, run_cfg: RunConfig) -> int:
    rate_error = (result.observed_rate_hz - run_cfg.rate_hz) / run_cfg.rate_hz
    logging.info("Run elapsed: %.3f s", result.elapsed_s)
    logging.info(
        "Samples per channel: %d (total device reported: %d)",
        result.samples_per_channel,
        result.total_acquired_per_channel,
    )
    logging.info(
        "Observed rate: %.3f Hz (error %.2f%%)",
        result.observed_rate_hz,
        rate_error * 100.0,
    )
    logging.info(
        "Loop latency mean/p95/max: %.3f / %.3f / %.3f ms",
        result.latency_mean_s * 1000.0,
        result.latency_p95_s * 1000.0,
        result.latency_max_s * 1000.0,
    )
    logging.info(
        "Loop latency std dev: %.3f ms (alerts triggered: %d)",
        result.latency_std_s * 1000.0,
        result.latency_alerts,
    )
    logging.info("Max backlog per channel: %d samples", result.max_backlog_samples)
    logging.info(
        "Iterations processed: %d | Stale iterations: %d",
        result.iterations,
        result.stale_iterations,
    )
    if run_cfg.log_path is not None:
        logging.info("Detailed metrics logged to %s", run_cfg.log_path)
    within_tolerance = abs(rate_error) <= run_cfg.rate_tolerance
    backlog_ok = result.max_backlog_samples == 0
    latency_ok = result.latency_alerts == 0
    stale_ok = result.stale_iterations == 0
    if not within_tolerance:
        logging.error(
            "Rate error %.2f%% exceeds tolerance %.2f%%",
            rate_error * 100.0,
            run_cfg.rate_tolerance * 100.0,
        )
    if not backlog_ok:
        logging.error("Detected backlog of %d samples", result.max_backlog_samples)
    if not latency_ok:
        logging.error(
            "Latency alert threshold (%.1f ms) exceeded %d times",
            run_cfg.latency_alert_s * 1000.0,
            result.latency_alerts,
        )
    if not stale_ok:
        logging.error("Detected %d stale read iterations", result.stale_iterations)
    if within_tolerance and backlog_ok and latency_ok and stale_ok:
        logging.info("Result: PASS")
        return 0
    return 1


def main() -> int:
    args = _parse_args()
    _configure_logging(args.log_level)
    current_cfg = _build_current_config(args)
    bridge_cfg = _build_bridge_config(args)
    run_cfg = _build_run_config(args)
    missing_devices: list[str] = []
    if run_cfg.require_pt and not current_cfg.physical_channels:
        missing_devices.append("NI-9208 current module (PT card)")
    if run_cfg.require_lc and not bridge_cfg.physical_channels:
        missing_devices.append("NI-9237 bridge module (load cells)")
    if missing_devices:
        error_msg = "Required devices not detected: " + ", ".join(missing_devices)
        logging.error(error_msg)
        return 1
    logging.info("Starting NI poller at %d Hz for %.1f s", run_cfg.rate_hz, run_cfg.duration_s)
    try:
        with nidaqmx.Task() as task:
            _configure_task(task, run_cfg.rate_hz, run_cfg.buffer_seconds, current_cfg, bridge_cfg)
            task.control(TaskMode.TASK_COMMIT)
            _run_calibrations(task, bridge_cfg, run_cfg)
            logging.info("Starting acquisition")
            task.start()
            try:
                result = _poll_loop(task, run_cfg, current_cfg, bridge_cfg)
            finally:
                task.stop()
    except KeyboardInterrupt:
        logging.warning("Interrupted by user")
        return 130
    except Exception as exc:  # pragma: no cover - hardware-specific exceptions
        logging.error("Acquisition failed: %s", exc)
        return 1
    return _summarize(result, run_cfg)


if __name__ == "__main__":
    sys.exit(main())