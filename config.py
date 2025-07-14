# Main system configuration
DAQ_HOST = "192.168.8.236"
DAQ_GRPC_PORT = 31763
NODE_HOST = "localhost"
NODE_TCP_PORT = 5001

# Device configuration
DEVICE_CHASSIS = "cDAQ9189-2462EFD"  # Actual device name from NI MAX
ACTIVE_DEVICES = ["pt_card"]  # List of active device modules

# PT Calibration Configuration - now loaded from interface_config.json
# This is kept for backward compatibility but can be overridden by the JSON config
PT_CALIBRATIONS = [
    {
        'channels': list(range(0, 16)),  # All 16 channels default to 0-10 ksi
        'min_ma': 4.0,
        'max_ma': 20.0,
        'min_ksi': 0.0,
        'max_ksi': 10.0
    }
] 