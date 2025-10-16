# NI-9237 4-channel bridge/strain/load cell configuration
import nidaqmx
from nidaqmx.constants import AcquisitionType, BridgeConfiguration, ExcitationSource, BridgeUnits
from typing import Dict, List, Any
from .base_device import BaseDevice
import nidaqmx.system
import logging
import json
from pathlib import Path

logger = logging.getLogger(__name__)

class LCCard(BaseDevice):
    """NI-9237 4-channel bridge input configuration (V/V).

    This class configures the NI-9237 for full-bridge measurements using internal excitation.
    Data is reported per-channel as average V/V over the acquired window. Any channel can be
    converted to engineering units downstream using known calibration factors.
    """
    def __init__(self, chassis: str, module_slot: int):
        super().__init__(chassis, module_slot)
        self.device_name = self.module_name
        self.product_type = "Unknown"
        # NI-9237 has 4 bridge input channels
        self.channel_count = 4
        # Robust cadence: 100 Hz device sample rate, app updates ~10 Hz
        self.sample_rate = 100
        self.samples_per_channel = 10

        # Attempt to autodetect the NI-9237 module on the chassis
        try:
            system = nidaqmx.system.System.local()
            for dev in system.devices:
                if dev.name.startswith(chassis) and "9237" in dev.product_type:
                    self.module_name = dev.name
                    try:
                        self.module_slot = int(dev.name.split('Mod')[-1])
                    except Exception:
                        pass
                    self.device_name = self.module_name
                    self.product_type = dev.product_type
                    break
        except Exception:
            pass

        # Load LC calibration config
        self.lc_config = {}
        self.tare_offsets = {}
        self._load_lc_config()

    def configure_channels(self, task: nidaqmx.Task) -> None:
        """Configure NI-9237 bridge channels as V/V with internal excitation."""
        channels = f"{self.device_name}/ai0:{self.channel_count-1}"
        # Typical defaults: full-bridge, internal excitation 2.5 V, nominal resistance 350 Ω
        # NI-9237 supports narrow V/V range; -25 mV/V .. +25 mV/V is typical
        task.ai_channels.add_ai_bridge_chan(
            physical_channel=channels,
            min_val=-0.025,  # -25 mV/V
            max_val=0.025,   #  25 mV/V
            units=BridgeUnits.VOLTS_PER_VOLT,
            bridge_config=BridgeConfiguration.FULL_BRIDGE,
            voltage_excit_source=ExcitationSource.INTERNAL,
            voltage_excit_val=2.5,
            nominal_bridge_resistance=350.0,
        )

    def configure_timing(self, task: nidaqmx.Task) -> None:
        # Continuous sampling; allocate a larger buffer and read windows per tick
        task.timing.cfg_samp_clk_timing(
            rate=self.sample_rate,
            sample_mode=AcquisitionType.CONTINUOUS,
            samps_per_chan=self.sample_rate * 5
        )
        try:
            task.in_stream.input_buf_size = int(self.sample_rate * 20)
        except Exception:
            pass

    @property
    def device_info(self) -> Dict[str, Any]:
        return {
            'device_type': 'LC Card (NI-9237)',
            'module': self.module_name,
            'channels': self.channel_count,
            'sample_rate': self.sample_rate
        }

    def _load_lc_config(self) -> None:
        """Load LC channel configuration from lc_channels.json"""
        try:
            config_path = Path(__file__).parent.parent / 'lc_channels.json'
            with open(config_path, 'r') as f:
                data = json.load(f)
                for channel_data in data.get('channels', []):
                    lc_info = channel_data.get('lc', {})
                    lc_id = lc_info.get('id')
                    if lc_id is not None:
                        # Map id (1-6) to channel index (0-3, only first 4 are valid for NI-9237)
                        channel_idx = lc_id - 1
                        if 0 <= channel_idx < self.channel_count:
                            self.lc_config[channel_idx] = lc_info
                            # Initialize tare offset to the value from config
                            cal = lc_info.get('calibration', {})
                            self.tare_offsets[channel_idx] = cal.get('tare_offset', 0.0)
                logger.info(f"Loaded LC config for {len(self.lc_config)} channels")
        except Exception as e:
            logger.warning(f"Could not load LC config: {e}")

    def _get_sensor_info(self, channel: int) -> Dict[str, str]:
        """Get sensor info for a channel"""
        if channel in self.lc_config:
            lc = self.lc_config[channel]
            return {
                'name': lc.get('name', f'Load Cell {channel}'),
                'id': lc.get('short_name', f'LC{channel}'),
                'group': 'loadcell'
            }
        return {
            'name': f'Load Cell {channel}',
            'id': f'LC{channel}',
            'group': 'loadcell'
        }

    def convert_to_lbf(self, v_per_v: float, channel: int) -> float:
        """Convert V/V to lbf using calibration: lbf = (slope * v_per_v + offset) - tare_offset

        Fallback behavior when explicit slope is not provided:
        - Use channel range's max as capacity (lbf)
        - Assume rated output 3.0 mV/V (Sensortronics 60001 typical)
        - slope = capacity / (rated_output_mV_per_V / 1000)  [lbf per V/V]
        - Apply optional mechanical_gain factor if present
        """
        if channel not in self.lc_config:
            return 0.0

        lc_info = self.lc_config[channel]
        cal = lc_info.get('calibration', {})

        slope = cal.get('slope')
        if slope is None:
            # Derive slope from capacity and rated output when not provided
            rng = lc_info.get('range') or [0.0, 0.0]
            try:
                capacity_lbf = float(rng[1]) if isinstance(rng, list) and len(rng) > 1 else 0.0
            except Exception:
                capacity_lbf = 0.0
            rated_output_mvv = float(cal.get('rated_output_mV_per_V', 3.0))  # Sensortronics 60001 nominal
            mech_gain = float(cal.get('mechanical_gain', 1.0))
            if rated_output_mvv <= 0.0:
                rated_output_mvv = 3.0
            if capacity_lbf > 0.0:
                slope = mech_gain * (capacity_lbf / (rated_output_mvv / 1000.0))
            else:
                # Sensible default for 1k lb with 3 mV/V
                slope = mech_gain * (1000.0 / 0.003)

        offset = float(cal.get('offset', 0.0))
        tare = float(self.tare_offsets.get(channel, 0.0))

        # y = mx + b - tare
        lbf = (float(slope) * float(v_per_v) + offset) - tare
        return lbf

    def tare(self, baseline_raw_data: List[List[float]]) -> None:
        """Set tare offsets based on current readings
        
        Args:
            baseline_raw_data: Raw V/V data from channels (same format as process_data input)
        """
        for ch_idx in range(min(self.channel_count, len(baseline_raw_data))):
            samples = baseline_raw_data[ch_idx] if isinstance(baseline_raw_data[ch_idx], list) else []
            if not samples:
                continue
            
            avg_v_per_v = sum(samples) / len(samples)
            
            # Calculate current lbf without tare
            if ch_idx in self.lc_config:
                cal = self.lc_config[ch_idx].get('calibration', {})
                slope = cal.get('slope', 1000000.0)
                offset = cal.get('offset', 0.0)
                
                # Current reading without tare
                current_lbf = slope * avg_v_per_v + offset
                
                # Set tare to this value so future readings are relative to this baseline
                self.tare_offsets[ch_idx] = current_lbf
                
                lc_name = self.lc_config[ch_idx].get('name', f'LC{ch_idx}')
                logger.info(f"Tared {lc_name} (ch{ch_idx}): offset = {current_lbf:.3f} lbf")
        
        logger.info(f"Tare complete for {len(self.tare_offsets)} load cell channels")

    def process_data(self, raw_data: List[List[float]]) -> Dict[str, Any]:
        result: List[Dict[str, Any]] = []
        if not isinstance(raw_data, list):
            return {'channels': result}
        for ch_idx in range(min(self.channel_count, len(raw_data))):
            samples = raw_data[ch_idx] if isinstance(raw_data[ch_idx], list) else []
            if not samples:
                avg_v_per_v = 0.0
            else:
                avg_v_per_v = sum(samples) / len(samples)
            
            info = self._get_sensor_info(ch_idx)
            status = 'ok'
            
            # Convert to lbf using calibration
            lbf = self.convert_to_lbf(avg_v_per_v, ch_idx)
            
            result.append({
                'channel': ch_idx,
                'name': info['name'],
                'id': info['id'],
                'group': info['group'],
                'v_per_v': round(avg_v_per_v, 6),
                'lbf': round(lbf, 3),
                'status': status,
                'units': 'lbf'
            })
        return {'channels': result}


