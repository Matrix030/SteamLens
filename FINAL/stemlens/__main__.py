#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
__main__.py - Entry point for stemlens when run as a module
"""

import os
import sys
import streamlit.web.cli as stcli

if __name__ == "__main__":
    # Add the parent directory to the sys.path
    sys.path.insert(0, os.path.abspath(os.path.dirname(os.path.dirname(__file__))))
    
    # Get the path to app.py
    app_path = os.path.join(os.path.dirname(__file__), "app.py")
    
    # Launch the streamlit app
    sys.argv = ["streamlit", "run", app_path]
    sys.exit(stcli.main()) 