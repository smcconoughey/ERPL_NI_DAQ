# Example template for adding a new device
# Copy this file and modify for your specific device

import nidaqmx
from nidaqmx.constants import AcquisitionType
from typing import Dict, List, Any
from .base_device import BaseDevice

class ExampleDevice(BaseDevice):
    """Example device configuration - replace with your device"""
    
    def __init__(self, chassis: str, module_slot: int = 1):
        super().__init__(chassis, module_slot)
        
        # Configure your device-specific parameters
        self.channel_count = 8  # Number of channels
        self.sample_rate = 1000  # Samples per second
        self.samples_per_read = 100
        
        # Add device-specific range/configuration
        self.min_value = -10.0
        self.max_value = 10.0
        
    def configure_channels(self, task: nidaqmx.Task) -> None:
        """Configure device-specific channels"""
        for i in range(self.channel_count):
            channel_name = f"{self.module_name}/ai{i}"
            
            # Replace with your channel type:
            # - add_ai_voltage_chan() for voltage
            # - add_ai_current_chan() for current
            # - add_ai_thrmcpl_chan() for thermocouple
            # - add_ai_strain_gage_chan() for strain gauge
            # etc.
            
            task.ai_channels.add_ai_voltage_chan(
                channel_name,
                min_val=self.min_value,
                max_val=self.max_value
            )
            
    def configure_timing(self, task: nidaqmx.Task) -> None:
        """Configure device timing"""
        task.timing.cfg_samp_clk_timing(
            rate=self.sample_rate,
            sample_mode=AcquisitionType.CONTINUOUS
        )
        
    def process_data(self, raw_data: List[List[float]]) -> Dict[str, Any]:
        """Process device data into structured format"""
        if not raw_data or len(raw_data) != self.channel_count:
            return {"error": "Invalid data format"}
            
        # Process your data here
        channel_data = {}
        for i, ch_data in enumerate(raw_data):
            if ch_data:
                # Calculate whatever metrics you need
                avg_value = sum(ch_data) / len(ch_data)
                channel_data[f"ch{i:02d}_value"] = avg_value
                channel_data[f"ch{i:02d}_raw"] = ch_data[-1]  # Last sample
            else:
                channel_data[f"ch{i:02d}_value"] = 0.0
                channel_data[f"ch{i:02d}_raw"] = 0.0
                
        return {
            "device_type": "example_device",  # Change this
            "module": self.module_name,
            "channels": channel_data,
            "sample_rate": self.sample_rate,
            "samples_processed": len(raw_data[0]) if raw_data[0] else 0
        }
        
    @property
    def device_info(self) -> Dict[str, Any]:
        """Return device information"""
        return {
            "device_type": "Example Device",  # Change this
            "module": self.module_name,
            "channels": self.channel_count,
            "sample_rate": self.sample_rate,
            "value_range": f"{self.min_value} to {self.max_value}",
            "samples_per_read": self.samples_per_read
        }

# To add this device:
# 1. Copy this file to devices/your_device_name.py
# 2. Modify class name and implementation
# 3. Add to devices/device_registry.py:
#    from .your_device_name import YourDeviceName
#    _devices["your_device"] = YourDeviceName
# 4. Add "your_device" to ACTIVE_DEVICES in config.py 