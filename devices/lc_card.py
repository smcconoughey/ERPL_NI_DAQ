# NI-9237 4-channel bridge/strain/load cell configuration
import nidaqmx
from nidaqmx.constants import AcquisitionType, BridgeConfiguration, ExcitationSource, BridgeUnits
from typing import Dict, List, Any
from .base_device import BaseDevice
import nidaqmx.system
import logging

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
        self.sample_rate = 1200
        self.samples_per_channel = 1200

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

    def configure_channels(self, task: nidaqmx.Task) -> None:
        """Configure NI-9237 bridge channels as V/V with internal excitation."""
        channels = f"{self.device_name}/ai0:{self.channel_count-1}"
        # Typical defaults: full-bridge, internal excitation 2.5 V, nominal resistance 350 Ω
        task.ai_channels.add_ai_bridge_chan(
            physical_channel=channels,
            min_val=-1.0,  # V/V
            max_val=1.0,
            units=BridgeUnits.VOLTS_PER_VOLT,
            bridge_cfg=BridgeConfiguration.FULL_BRIDGE,
            voltage_excit_source=ExcitationSource.INTERNAL,
            voltage_excit_val=2.5,
            nominal_bridge_resistance=350.0,
        )

    def configure_timing(self, task: nidaqmx.Task) -> None:
        task.timing.cfg_samp_clk_timing(
            rate=self.sample_rate,
            sample_mode=AcquisitionType.FINITE,
            samps_per_chan=self.samples_per_channel
        )

    @property
    def device_info(self) -> Dict[str, Any]:
        return {
            'device_type': 'LC Card (NI-9237)',
            'module': self.module_name,
            'channels': self.channel_count,
            'sample_rate': self.sample_rate
        }

    def _get_sensor_info(self, channel: int) -> Dict[str, str]:
        # Minimal identities; UI can map names if desired
        return {
            'name': f'Load Cell {channel}',
            'id': f'LC{channel}',
            'group': 'loadcell'
        }

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
            result.append({
                'channel': ch_idx,
                'name': info['name'],
                'id': info['id'],
                'group': info['group'],
                'v_per_v': round(avg_v_per_v, 6),
                'status': status,
                'units': 'V/V'
            })
        return {'channels': result}


