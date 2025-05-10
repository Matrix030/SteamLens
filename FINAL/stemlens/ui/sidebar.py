#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
sidebar.py - UI for the sidebar
Handles configuration, themes, and timing metrics
"""

import os
import datetime
import streamlit as st
from typing import Dict, Any

from ..config.app_config import DEFAULT_THEME_FILE
from ..utils.system_utils import format_time

def render_sidebar() -> None:
    """Render the sidebar UI
    
    Displays configuration options, theme file uploader, timing metrics, and dashboard links
    """
    st.sidebar.header("Configuration")
    
    # Theme file uploader
    theme_file = st.sidebar.file_uploader(
        "Upload Theme File (game_themes.json)",
        type=["json"],
        help="This file maps game app IDs to their themes"
    )
    
    if theme_file:
        # Save the theme file
        with open(DEFAULT_THEME_FILE, "wb") as f:
            f.write(theme_file.getbuffer())
        st.sidebar.success("✅ Theme file uploaded successfully!")
    
    # Check if theme file exists
    theme_file_exists = os.path.exists(DEFAULT_THEME_FILE)
    if not theme_file_exists:
        st.sidebar.warning("⚠️ Theme file not found. Please upload a theme file first.")
    
    # Display dashboard links if available
    st.sidebar.markdown("---")
    st.sidebar.header("🔗 Dask Dashboards")
    
    if 'process_dashboard_link' in st.session_state:
        st.sidebar.markdown(f"**[Processing Dashboard]({st.session_state.process_dashboard_link})** ✨")
    
    if 'summarize_dashboard_link' in st.session_state:
        st.sidebar.markdown(f"**[Summarization Dashboard]({st.session_state.summarize_dashboard_link})** ✨")
    
    if 'process_dashboard_link' not in st.session_state and 'summarize_dashboard_link' not in st.session_state:
        st.sidebar.info("Dashboard links will appear here once you start processing or summarization.")
    
    # Display timing information
    if 'timing_data' in st.session_state:
        render_timing_metrics(st.session_state.timing_data)
    
    # Footer
    st.sidebar.markdown("---")
    st.sidebar.info("""
    **SteamLens Sentiment Analysis** processes Steam reviews to identify what players love and hate about each theme.
    Separating positive and negative reviews provides clearer insights into player sentiment.
    """)

def render_timing_metrics(timing_data: Dict[str, Any]) -> None:
    """Render the timing metrics
    
    Args:
        timing_data (dict): Dictionary containing timing data
    """
    st.sidebar.markdown("---")
    st.sidebar.header("⏱️ Execution Time Metrics")
    
    # Calculate global elapsed time
    current_time = datetime.datetime.now().timestamp()
    timing_data['global_end_time'] = current_time
    global_elapsed_time = current_time - timing_data['global_start_time']
    
    # Calculate timing data only if operations were performed
    process_time = None
    if timing_data['process_start_time'] and timing_data['process_end_time']:
        process_time = timing_data['process_end_time'] - timing_data['process_start_time']
    
    summarize_time = None
    if timing_data['summarize_start_time'] and timing_data['summarize_end_time']:
        summarize_time = timing_data['summarize_end_time'] - timing_data['summarize_start_time']
    
    # Display the timing metrics
    st.sidebar.markdown(f"**Total Session Time:** {format_time(global_elapsed_time)}")
    st.sidebar.markdown(f"**Processing Time:** {format_time(process_time)}")
    st.sidebar.markdown(f"**Summarization Time:** {format_time(summarize_time)}")
    
    # If both processing and summarization were completed, show total analytical time
    if process_time and summarize_time:
        total_analytical_time = process_time + summarize_time
        st.sidebar.markdown(f"**Total Analytical Time:** {format_time(total_analytical_time)}")
    
    # Add a timestamp of when the analysis was performed
    if timing_data['global_end_time']:
        timestamp = datetime.datetime.fromtimestamp(timing_data['global_end_time']).strftime('%Y-%m-%d %H:%M:%S')
        st.sidebar.markdown(f"**Analysis Timestamp:** {timestamp}") 