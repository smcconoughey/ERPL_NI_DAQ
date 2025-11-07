# NI-9208 16-channel current measurement PT card configuration
import nidaqmx
from nidaqmx.constants import AcquisitionType, TerminalConfiguration
from typing import Dict, List, Any
from .base_device import BaseDevice
import json
import os
import nidaqmx.system
import logging

logger = logging.getLogger(__name__)

class PTCard(BaseDevice):
    """NI-9208 16-channel current measurement configuration"""
    
    def __init__(self, chassis: str, module_slot: int):
        super().__init__(chassis, module_slot)
        # Prefer autodetected NI-9208 module on the chassis; fall back to provided slot
        self.device_name = self.module_name
        self.product_type = "Unknown"
        try:
            system = nidaqmx.system.System.local()
            for dev in system.devices:
                if dev.name.startswith(chassis) and "9208" in dev.product_type:
                    self.module_name = dev.name
                    # Update slot from autodetected module name suffix '...ModN'
                    try:
                        self.module_slot = int(dev.name.split('Mod')[-1])
                    except Exception:
                        pass
                    self.device_name = self.module_name
                    self.product_type = dev.product_type
                    break
        except Exception:
            # If autodetection fails, keep the provided module slot
            pass
        self.channel_count = 16
        # Default to 100 Hz for true 100 Hz acquisition (will be hardware-timed)
        self.sample_rate = 100.0
        # Main loop runs at 10 Hz, reads 10 samples per iteration
        self.samples_per_channel = 10
        # Per-channel tare offset in PSI
        self.tare_offsets: Dict[int, float] = {}
        self.tared = False
        self.load_config()
    
    def load_config(self):
        try:
            config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'interface_config.json')
            with open(config_path, 'r') as f:
                config = json.load(f)
            
            # Changed from pt_sensors to sensors
            self.sensor_config = {sensor['channel']: sensor for sensor in config['sensors']}
            logger.info(f"Loaded {len(self.sensor_config)} PT sensor configs from interface_config.json")
        except Exception as e:
            logger.warning(f"Could not load sensor config: {e}")
            self.sensor_config = {}

    def tare(self, baseline_raw_data: List[List[float]], ambient_psi: float = 14.7) -> None:
        """Set tare offsets per-channel based on current readings.

        For gauge PTs, the expected zero corresponds to 4 mA. If a channel is
        currently reading e.g. 3.97 mA (~> 0 psi mapping would floor to 0), we
        store the current computed psi as tare so subsequent readings zero at
        the present operating point.
        """
        try:
            for channel_idx, channel_data in enumerate(baseline_raw_data or []):
                if not isinstance(channel_data, list) or not channel_data:
                    continue
                avg_current_a = sum(channel_data) / len(channel_data)
                current_ma = avg_current_a * 1000.0
                try:
                    psi_no_tare = self._convert_no_tare(current_ma, channel_idx)
                except Exception:
                    continue
                # Store the present psi as the tare reference
                self.tare_offsets[channel_idx] = float(psi_no_tare)
            self.tared = True
        except Exception:
            # Non-fatal; leave existing offsets as-is
            pass

    def _convert_no_tare(self, current_ma, channel):
        if channel in self.sensor_config:
            config = self.sensor_config[channel]
            cal = config.get('calibration', {})

            zero_ma = cal.get('zero_ma', 4.0)
            span_ma = 16.0
            max_psi = cal.get('max_psi', 10000.0)

            psi = (current_ma - zero_ma) * (max_psi / span_ma)
            return psi
        else:
            raise ValueError(f"Unconfigured PT channel {channel}: missing calibration in interface_config.json")
    
    def configure_channels(self, task: nidaqmx.Task) -> None:
        channels = f"{self.device_name}/ai0:{self.channel_count-1}"
        
        task.ai_channels.add_ai_current_chan(
            channels,
            min_val=-0.022,
            max_val=0.022,
            terminal_config=TerminalConfiguration.DEFAULT
        )
    
    def configure_timing(self, task: nidaqmx.Task) -> None:
        """Configure hardware-timed continuous sampling with proper buffer sizing.
        
        Per NI literature:
        - Use HIGH_SPEED mode for 100 Hz (2 ms/channel conversion)
        - HIGH_RESOLUTION mode (52 ms/channel) is better for < 20 Hz but slower
        - Hardware-timed continuous sampling avoids stale/duplicate data
        """
        task.timing.cfg_samp_clk_timing(
            rate=self.sample_rate,
            sample_mode=AcquisitionType.CONTINUOUS,
            samps_per_chan=int(self.sample_rate * 10)  # 10 seconds onboard buffer
        )
        try:
            # 60 seconds of host buffer for safety
            task.in_stream.input_buf_size = int(self.sample_rate * 60)
        except Exception:
            pass
        
        # Configure ADC timing mode based on sample rate
        # For 100 Hz: HIGH_SPEED mode (2 ms/channel, up to 500 S/s)
        # For ≤ 10 Hz: Could use HIGH_RESOLUTION (52 ms/channel, ~19 S/s) for better accuracy
        try:
            if self.sample_rate >= 50.0:
                # High-speed mode required for 100 Hz across multiple channels
                task.ai_channels.all.ai_adc_timing_mode = nidaqmx.constants.ADCTimingMode.HIGH_SPEED
                logger.info("NI-9208: Configured HIGH_SPEED ADC mode for 100 Hz sampling")
            else:
                # High-resolution mode for better accuracy at low rates (10 Hz)
                task.ai_channels.all.ai_adc_timing_mode = nidaqmx.constants.ADCTimingMode.HIGH_RESOLUTION
                logger.info("NI-9208: Configured HIGH_RESOLUTION ADC mode for low-rate sampling")
        except Exception as e:
            logger.warning(f"Could not set ADC timing mode: {e}")
        
        # Disable digital filtering to reduce latency
        try:
            task.ai_channels.all.ai_digital_filter_enable = False
        except Exception:
            pass
    
    @property
    def device_info(self) -> Dict[str, Any]:
        return {
            'device_type': 'PT Card (NI-9208)',
            'module': self.module_name,
            'channels': self.channel_count,
            'sample_rate': self.sample_rate
        }
    
    def convert_to_pressure(self, current_ma, channel):
        # Robust conversion that always returns a non-negative PSI
        try:
            psi = self._convert_no_tare(current_ma, channel)
        except Exception:
            # Fallback for unconfigured channels
            psi = (float(current_ma) - 4.0) * (10000.0 / 16.0)
        # Apply per-channel tare offset
        psi -= float(self.tare_offsets.get(channel, 0.0))
        return max(0.0, psi)
    
    def get_sensor_info(self, channel):
        if channel in self.sensor_config:
            config = self.sensor_config[channel]
            return {
                'name': config['name'],
                'id': config['id'],
                'group': config['group']
            }
        else:
            return {
                'name': f'Channel {channel}',
                'id': f'CH{channel}',
                'group': 'other'
            }
    
    def process_data(self, raw_data: List[List[float]]) -> Dict[str, Any]:
        if not (isinstance(raw_data, list) and raw_data and isinstance(raw_data[0], list)):
            return {'channels': [], 'samples': []}

        processed_channels: List[Dict[str, Any]] = []
        per_sample_records: List[List[Dict[str, Any]]] = []

        for channel_idx, channel_data in enumerate(raw_data):
            if not isinstance(channel_data, list) or not channel_data:
                continue

            # Use most recent sample for responsive UI updates
            latest_current = channel_data[-1]
            current_ma = latest_current * 1000
            status = 'ok'
            if current_ma < 3.0 or current_ma > 25.0:
                status = 'disconnected'
            try:
                psi = self.convert_to_pressure(current_ma, channel_idx) if status == 'ok' else 0.0
            except Exception:
                status = 'unconfigured'
                psi = 0.0
            sensor_info = self.get_sensor_info(channel_idx)

            processed_channels.append({
                'channel': channel_idx,
                'name': sensor_info['name'],
                'id': sensor_info['id'],
                'group': sensor_info['group'],
                'current_ma': round(current_ma, 3),
                'status': status,
                'pressure_psi': round(psi, 3),
                'units': 'psi'
            })

            for sample_idx, sample_val in enumerate(channel_data):
                while len(per_sample_records) <= sample_idx:
                    per_sample_records.append([])

                sample_ma = sample_val * 1000.0
                sample_status = 'ok'
                if sample_ma < 3.0 or sample_ma > 25.0:
                    sample_status = 'disconnected'
                try:
                    sample_psi = self.convert_to_pressure(sample_ma, channel_idx) if sample_status == 'ok' else 0.0
                except Exception:
                    sample_status = 'unconfigured'
                    sample_psi = 0.0

                per_sample_records[sample_idx].append({
                    'channel': channel_idx,
                    'current_ma': round(sample_ma, 3),
                    'pressure_psi': round(sample_psi, 3),
                    'status': sample_status,
                })

        return {'channels': processed_channels, 'samples': per_sample_records}
