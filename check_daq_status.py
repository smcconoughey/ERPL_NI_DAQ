#!/usr/bin/env python3
# DAQ Status Checker - diagnose device availability and resource issues

import nidaqmx
import nidaqmx.system
from config import DEVICE_CHASSIS

def check_daq_status():
    """Check DAQ device status and availability"""
    print("=" * 50)
    print("  DAQ Device Status Check")
    print("=" * 50)
    
    try:
        # Get system information
        system = nidaqmx.system.System.local()
        print(f"NI-DAQmx Version: {system.driver_version}")
        print()
        
        # List all devices
        print("Available Devices:")
        for device in system.devices:
            print(f"  - {device.name}")
            try:
                print(f"    Product Type: {device.product_type}")
                print(f"    Serial Number: {device.dev_serial_num}")
                print(f"    Is Simulated: {device.dev_is_simulated}")
                
                # Check if our target device
                if DEVICE_CHASSIS in device.name:
                    print(f"    ✓ TARGET DEVICE FOUND: {device.name}")
                    
                    # Probe for NI-9208 and NI-9237 modules explicitly with proper channel types
                    try:
                        with nidaqmx.Task() as test_task:
                            found_any = False
                            for mod in system.devices:
                                if mod.name.startswith(device.name + "Mod"):
                                    if "9208" in mod.product_type:
                                        # Current measurement
                                        test_task.ai_channels.add_ai_current_chan(f"{mod.name}/ai0")
                                        found_any = True
                                        print(f"    ✓ NI-9208 OK: {mod.name} ai0 current")
                                    elif "9237" in mod.product_type:
                                        # Bridge/strain module expects bridge types, not plain voltage/current
                                        print(f"    ℹ︎ NI-9237 detected: {mod.name} (bridge/strain). Skipping current/voltage test.")
                            if not found_any:
                                print("    ⚠️ No NI-9208 module detected on this chassis.")
                    except Exception as e:
                        print(f"    ❌ Module probe error: {e}")
                        print("    💡 Try closing NI MAX or other DAQ applications")
                        
                        # Try to reset the device
                        try:
                            device.reset_device()
                            print("    ✓ Device reset successful")
                        except Exception as reset_err:
                            print(f"    ❌ Device reset failed: {reset_err}")
                            
            except Exception as e:
                print(f"    Error getting device info: {e}")
            print()
            
    except Exception as e:
        print(f"❌ Error accessing DAQ system: {e}")
        print("💡 Make sure NI-DAQmx Runtime is installed")
        print("💡 Check device connection and IP address")
        
    print("=" * 50)

if __name__ == "__main__":
    check_daq_status() 