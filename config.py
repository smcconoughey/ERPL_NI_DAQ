# Main system configuration
DAQ_HOST = "192.168.8.236"
DAQ_GRPC_PORT = 31763
NODE_HOST = "localhost"
NODE_TCP_PORT = 5001

# Debugging
DEBUG_ENABLE = True            # Master debug switch
DEBUG_RAW_SUMMARY = True       # Log per-read summaries (min/max/avg per channel)
DEBUG_RAW_SAMPLES = False      # Log a few raw samples per channel periodically
DEBUG_SAMPLE_EVERY_N = 50      # How often to log raw samples (reads)

# Device configuration
DEVICE_CHASSIS = "cDAQ9189-2462EFD"  # Actual device name from NI MAX
# Enable both PT and LC cards so both stream in the UI
ACTIVE_DEVICES = ["pt_card", "lc_card"]  # List of active device modules

# Per-device slot overrides (fallback if autodetect fails)
PT_MODULE_SLOT = 2
LC_MODULE_SLOT = 1
MODULE_SLOT = 1  # legacy fallback

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