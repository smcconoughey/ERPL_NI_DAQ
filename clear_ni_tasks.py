#!/usr/bin/env python3
"""Clear all persisted NI-DAQmx tasks to prevent resource conflicts."""

import nidaqmx.system
import sys

def clear_all_tasks():
    """Delete all persisted NI-DAQmx tasks."""
    system = nidaqmx.system.System.local()
    
    print("=" * 60)
    print("  NI-DAQmx Task Cleanup")
    print("=" * 60)
    
    tasks = list(system.tasks)
    
    if not tasks:
        print("OK: No persisted tasks found - nothing to clean up")
        return
    
    print(f"\nFound {len(tasks)} persisted task(s):")
    for task in tasks:
        print(f"  - {task.name}")
    
    print("\nWARNING: These tasks may be reserving hardware resources.")
    print("         Deleting them will NOT affect your Python code.")
    print("         Automatically deleting all tasks...")
    print()
    
    deleted = 0
    failed = 0
    for task in tasks:
        try:
            task.delete()
            print(f"OK: Deleted {task.name}")
            deleted += 1
        except Exception as e:
            print(f"ERROR: Failed to delete {task.name}: {e}")
            failed += 1
    
    print(f"\nTask cleanup complete: {deleted} deleted, {failed} failed")

if __name__ == "__main__":
    clear_all_tasks()

