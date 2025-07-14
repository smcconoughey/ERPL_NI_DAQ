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
                    
                    # Try to create a simple task to test availability
                    try:
                        with nidaqmx.Task() as test_task:
                            test_task.ai_channels.add_ai_voltage_chan(f"{device.name}Mod1/ai0")
                            print("    ✓ Device is available (no resource conflicts)")
                    except Exception as e:
                        print(f"    ❌ Device resource error: {e}")
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