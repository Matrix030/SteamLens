#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
run_stemlens.py - Launcher script for SteamLens
Provides a simple way to start the SteamLens application
"""

import os
import sys
import subprocess

def main():
    """Main entry point for the SteamLens launcher"""
    print("Starting SteamLens Sentiment Analysis application...")
    
    # Get the current directory
    current_dir = os.path.abspath(os.path.dirname(__file__))
    steamlens_dir = os.path.join(current_dir, 'steamLens')
    
    # Add the current directory to the Python path so we can import steamLens
    sys.path.insert(0, current_dir)
    
    # Check if setup.py exists in steamLens directory
    setup_path = os.path.join(steamlens_dir, 'setup.py')
    if os.path.exists(setup_path):
        print(f"Found setup.py at {setup_path}")
        
        # Install the package in development mode if needed
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "install", "-e", steamlens_dir])
            print("SteamLens package installed/updated")
        except subprocess.CalledProcessError:
            print("Warning: Failed to install the package")
    else:
        print(f"Warning: setup.py not found at {setup_path}")
    
    # Run the Streamlit app by executing the steamLens module
    print("Launching the SteamLens application...")
    subprocess.run([sys.executable, "-m", "steamLens"], check=True)

if __name__ == "__main__":
    main() 