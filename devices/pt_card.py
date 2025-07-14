# NI-9208 16-channel current measurement PT card configuration
import nidaqmx
from nidaqmx.constants import AcquisitionType, CurrentUnits
from typing import Dict, List, Any
from .base_device import BaseDevice

class PTCard(BaseDevice):
    """NI-9208 16-channel current measurement configuration"""
    
    def __init__(self, chassis: str, module_slot: int = 1):
        super().__init__(chassis, module_slot)
        self.channel_count = 16
        self.sample_rate = 100  # Reduced from 500Hz to 100Hz for stability
        self.samples_per_read = 10  # Read very small chunks frequently
        self.buffer_size = 2000  # Much larger buffer to prevent overruns
        self.min_current = 0.0040  # -20 mA
        self.max_current = 0.020   # +20 mA
        
    def configure_channels(self, task: nidaqmx.Task) -> None:
        """Configure 16 current input channels"""
        for i in range(self.channel_count):
            channel_name = f"{self.module_name}/ai{i}"
            task.ai_channels.add_ai_current_chan(
                channel_name,
                min_val=self.min_current,
                max_val=self.max_current,
                units=CurrentUnits.AMPS
            )
            
    def configure_timing(self, task: nidaqmx.Task) -> None:
        """Configure finite sampling to prevent buffer overruns"""
        task.timing.cfg_samp_clk_timing(
            rate=self.sample_rate,
            sample_mode=AcquisitionType.FINITE,
            samps_per_chan=self.samples_per_read
        )
        
    def process_data(self, raw_data: List[List[float]]) -> Dict[str, Any]:
        """Process 16-channel current data"""
        if not raw_data:
            return {"error": "No data received"}
            
        # Ensure we have the right number of channels
        if len(raw_data) != self.channel_count:
            # Pad with zeros if we have fewer channels
            while len(raw_data) < self.channel_count:
                raw_data.append([0.0])
            # Truncate if we have more channels
            raw_data = raw_data[:self.channel_count]
            
        # Calculate averages and convert to mA
        channel_data = {}
        for i, ch_data in enumerate(raw_data):
            if ch_data and len(ch_data) > 0:
                # Handle both single values and lists
                if isinstance(ch_data, (int, float)):
                    avg_current = float(ch_data)
                else:
                    avg_current = sum(ch_data) / len(ch_data)
                    
                channel_data[f"ch{i:02d}_current_ma"] = avg_current * 1000  # Convert to mA
                channel_data[f"ch{i:02d}_current_a"] = avg_current  # Keep in Amps
            else:
                channel_data[f"ch{i:02d}_current_ma"] = 0.0
                channel_data[f"ch{i:02d}_current_a"] = 0.0
                
        return {
            "device_type": "pt_card",
            "module": self.module_name,
            "channels": channel_data,
            "sample_rate": self.sample_rate,
            "samples_processed": len(raw_data[0]) if raw_data[0] else 0
        }
        
    @property
    def device_info(self) -> Dict[str, Any]:
        """Return PT card device information"""
        return {
            "device_type": "PT Card (NI-9208)",
            "module": self.module_name,
            "channels": self.channel_count,
            "sample_rate": self.sample_rate,
            "current_range": f"{self.min_current*1000:.0f} to {self.max_current*1000:.0f} mA",
            "samples_per_read": self.samples_per_read,
            "buffer_size": self.buffer_size
        } 