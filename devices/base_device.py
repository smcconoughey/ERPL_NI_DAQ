# Base device configuration class
import nidaqmx
from abc import ABC, abstractmethod
from typing import Dict, List, Any

class BaseDevice(ABC):
    """Base class for all DAQ device configurations"""
    
    def __init__(self, chassis: str, module_slot: int):
        self.chassis = chassis
        self.module_slot = module_slot
        self.module_name = f"{chassis}Mod{module_slot}"
        self.task = None
        
    @abstractmethod
    def configure_channels(self, task: nidaqmx.Task) -> None:
        """Configure device-specific channels"""
        pass
        
    @abstractmethod
    def configure_timing(self, task: nidaqmx.Task) -> None:
        """Configure device-specific timing"""
        pass
        
    @abstractmethod
    def process_data(self, raw_data: List[List[float]]) -> Dict[str, Any]:
        """Process raw data into structured format"""
        pass
        
    @property
    @abstractmethod
    def device_info(self) -> Dict[str, Any]:
        """Return device information"""
        pass 