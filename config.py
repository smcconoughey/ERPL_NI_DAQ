# Main system configuration
DAQ_HOST = "192.168.8.236"
DAQ_GRPC_PORT = 31763
NODE_HOST = "localhost"
NODE_TCP_PORT = 5001

# Debugging
DEBUG_ENABLE = True            # Master debug switch
DEBUG_RAW_SUMMARY = True       # Log per-read summaries (min/max/avg per channel)
DEBUG_RAW_SAMPLES = False      # Log a few raw samples per channel periodically
DEBUG_SAMPLE_EVERY_N = 10      # How often to log raw samples (reads)

# Device configuration
DEVICE_CHASSIS = "cDAQ9189-2462EFD"  # Actual device name from NI MAX
ACTIVE_DEVICES = ["pt_card"]  # List of active device modules
MODULE_SLOT = 1  # Physical slot number for the active module(s)

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