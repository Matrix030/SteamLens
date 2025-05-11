#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
system_utils.py - System utilities for SteamLens
Includes functions for resource detection and system configuration
"""

import psutil

def get_system_resources():
    """Dynamically determine system resources
    
    Returns:
        dict: Dictionary containing worker_count, memory_per_worker, and total_memory
    """
    # Get available memory (in GB)
    total_memory = psutil.virtual_memory().total / (1024**3)
    
    # Get CPU count
    cpu_count = psutil.cpu_count(logical=False)  # Physical cores only
    if not cpu_count:
        cpu_count = psutil.cpu_count(logical=True)  # Logical if physical not available
    
    # Use 70% of available memory for Dask, split across workers
    dask_memory = int(total_memory * 0.7)
    
    # Determine optimal worker count (leave at least 1 core for system)
    worker_count = max(1, cpu_count - 1)
    
    # Memory per worker
    memory_per_worker = int(dask_memory / worker_count)
    
    return {
        'worker_count': worker_count,
        'memory_per_worker': memory_per_worker,
        'total_memory': total_memory
    }

def estimate_file_size(file):
    """Estimate size of file in GB
    
    Args:
        file: The file object to estimate the size of
        
    Returns:
        float: Size of the file in GB
    """
    return file.size / (1024**3)  # Convert to GB
    
def format_time(seconds):
    """Format time in seconds to a human-readable string
    
    Args:
        seconds (float): Time in seconds
        
    Returns:
        str: Formatted time string
    """
    if seconds is None:
        return "Not completed"
    
    if seconds < 60:
        return f"{seconds:.2f} seconds"
    elif seconds < 3600:
        minutes = seconds // 60
        remaining_seconds = seconds % 60
        return f"{int(minutes)} minutes, {remaining_seconds:.2f} seconds"
    else:
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        remaining_seconds = seconds % 60
        return f"{int(hours)} hours, {int(minutes)} minutes, {remaining_seconds:.2f} seconds" 