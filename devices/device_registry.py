# Device registry for managing different DAQ device types
from typing import Dict, List, Type
from .base_device import BaseDevice
from .pt_card import PTCard
from .lc_card import LCCard
from .tc_card import TCCard

class DeviceRegistry:
    """Registry for managing different DAQ device types"""
    
    _devices: Dict[str, Type[BaseDevice]] = {
        "pt_card": PTCard,
        "lc_card": LCCard,
        "tc_card": TCCard,
    }
    
    @classmethod
    def get_device_class(cls, device_type: str) -> Type[BaseDevice]:
        """Get device class by type name"""
        if device_type not in cls._devices:
            raise ValueError(f"Unknown device type: {device_type}")
        return cls._devices[device_type]
    
    @classmethod
    def create_device(cls, device_type: str, chassis: str, module_slot: int = 1) -> BaseDevice:
        """Create device instance by type"""
        device_class = cls.get_device_class(device_type)
        return device_class(chassis, module_slot)
    
    @classmethod
    def list_available_devices(cls) -> List[str]:
        """List all available device types"""
        return list(cls._devices.keys())
    
    @classmethod
    def register_device(cls, device_type: str, device_class: Type[BaseDevice]) -> None:
        """Register a new device type"""
        cls._devices[device_type] = device_class 