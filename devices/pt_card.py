# NI-9208 16-channel current measurement PT card configuration
import nidaqmx
from nidaqmx.constants import AcquisitionType, TerminalConfiguration
from typing import Dict, List, Any
from .base_device import BaseDevice
import json
import os

class PTCard(BaseDevice):
    """NI-9208 16-channel current measurement configuration"""
    
    def __init__(self, chassis: str, module_slot: int):
        super().__init__(chassis, module_slot)
        self.device_name = self.module_name
        self.channel_count = 16
        self.sample_rate = 500
        self.samples_per_channel = 500
        self.load_config()
    
    def load_config(self):
        try:
            config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'interface_config.json')
            with open(config_path, 'r') as f:
                config = json.load(f)
            
            # Changed from pt_sensors to sensors
            self.sensor_config = {sensor['channel']: sensor for sensor in config['sensors']}
        except Exception as e:
            print(f"Warning: Could not load sensor config: {e}")
            self.sensor_config = {}
    
    def configure_channels(self, task: nidaqmx.Task) -> None:
        channels = f"{self.device_name}/ai0:{self.channel_count-1}"
        
        task.ai_channels.add_ai_current_chan(
            channels,
            min_val=-0.022,
            max_val=0.022,
            terminal_config=TerminalConfiguration.DEFAULT
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
            'device_type': 'PT Card (NI-9208)',
            'module': self.module_name,
            'channels': self.channel_count,
            'sample_rate': self.sample_rate
        }
    
    def convert_to_pressure(self, current_ma, channel):
        if channel in self.sensor_config:
            config = self.sensor_config[channel]
            cal = config['calibration']
            
            # Linear interpolation from 4-20mA to PSI (changed from ksi)
            min_ma = cal['min_ma']
            max_ma = cal['max_ma']
            min_psi = cal['min_psi']  # Changed from min_ksi
            max_psi = cal['max_psi']  # Changed from max_ksi
            
            psi = min_psi + (current_ma - min_ma) * (max_psi - min_psi) / (max_ma - min_ma)
            return max(0.0, psi)  # Clamp to minimum 0 PSI
        else:
            # Default conversion for unconfigured channels
            default_psi = 0.0 + (current_ma - 4.0) * (10000.0 - 0.0) / (20.0 - 4.0)
            return max(0.0, default_psi)
    
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
        if isinstance(raw_data, list) and len(raw_data) > 0 and isinstance(raw_data[0], list):
            processed_channels = []
            for channel_idx, channel_data in enumerate(raw_data):
                if len(channel_data) > 0:
                    # Average the samples for better noise reduction
                    avg_current = sum(channel_data) / len(channel_data)
                    current_ma = avg_current * 1000
                    psi = self.convert_to_pressure(current_ma, channel_idx)  # Changed from ksi
                    sensor_info = self.get_sensor_info(channel_idx)
                    
                    processed_channels.append({
                        'channel': channel_idx,
                        'name': sensor_info['name'],
                        'id': sensor_info['id'],
                        'group': sensor_info['group'],
                        'current_ma': round(current_ma, 2),
                        'pressure_psi': round(psi, 2),  # Changed from pressure_ksi
                        'units': 'psi'  # Changed from ksi
                    })
            return {'channels': processed_channels}
        else:
            return {'channels': []} 