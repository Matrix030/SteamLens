#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
summarize_processor.py - Sentiment-based summarization processor
Manages the summarization of positive and negative reviews
"""

import time
import threading
import datetime
import streamlit as st
import pandas as pd
import dask
from typing import Dict, Any, Optional
from dask.distributed import Client, LocalCluster

from ..config.app_config import HARDWARE_CONFIG, DEFAULT_OUTPUT_PATH
from .summarization import prepare_partition, process_partition, update_main_progress

def summarize_report(final_report: pd.DataFrame) -> Optional[pd.DataFrame]:
    """Hardware-optimized GPU summarization phase with sentiment separation
    
    Args:
        final_report (DataFrame): Final report from the processing step
        
    Returns:
        DataFrame or None: Summarized report with positive and negative summaries or None if summarization fails
    """
    # Start phase timer for detailed timing
    phase_start_time = time.time()
    
    if final_report is None or final_report.empty:
        st.error("No report to summarize.")
        return None
    
    # Create progress indicators
    progress_placeholder = st.empty()
    status_placeholder = st.empty()
    dashboard_placeholder = st.empty()  # Add placeholder for dashboard link
    
    with progress_placeholder.container():
        status_text = st.empty()
        progress_bar = st.progress(0.0)
    
    # Use hardware_config from config module (with defaults)
    # Can be overridden or customized here if needed
    hardware_config = HARDWARE_CONFIG
    
    status_text.write(f"Starting optimized Dask cluster for sentiment-based summarization")
    
    # Use try/finally to ensure proper cleanup of Dask resources
    cluster = None
    client = None
    
    try:
        cluster = LocalCluster(
            n_workers=hardware_config['worker_count'], 
            threads_per_worker=2,
            memory_limit=hardware_config['memory_per_worker']
        )
        client = Client(cluster)
        status_text.write(f"Dask dashboard initialized")
        
        # Immediately display dashboard link for monitoring
        with dashboard_placeholder.container():
            st.success("✅ Dask Summarization Cluster Ready")
            st.markdown(f"**[Open Dask Dashboard]({client.dashboard_link})** (opens in new tab)")
            st.info("👁️ Monitor summarization tasks in real-time with this dashboard")
        
        # Store dashboard link in session state for later reference
        if 'summarize_dashboard_link' not in st.session_state:
            st.session_state.summarize_dashboard_link = client.dashboard_link
        
        # Determine optimal partition sizes - larger for better throughput
        n_workers = hardware_config['worker_count']
        partition_size = len(final_report) // n_workers
        partitions = []
        for i in range(n_workers):
            start_idx = i * partition_size
            end_idx = (i + 1) * partition_size if i < n_workers - 1 else len(final_report)
            partitions.append(dask.delayed(prepare_partition)(start_idx, end_idx, final_report))
            status_text.write(f"Prepared partition {i+1} with {end_idx-start_idx} items")
        
        # Schedule tasks
        status_text.write(f"Scheduling {n_workers} optimized partitions for sentiment analysis...")
        delayed_results = []
        for i in range(n_workers):
            delayed_result = dask.delayed(process_partition)(partitions[i], i, hardware_config)
            delayed_results.append(delayed_result)
        
        # Start timing
        start_time = time.time()
        
        # Submit tasks to cluster
        futures = client.compute(delayed_results)
        
        # Start progress monitor with minimal overhead
        stop_flag = [False]  # Use a list to make it mutable for the thread
        monitor_thread = threading.Thread(
            target=update_main_progress, 
            args=(futures, progress_bar, stop_flag, len(final_report))
        )
        monitor_thread.daemon = True
        monitor_thread.start()
        
        # Wait for computation
        try:
            status_text.write("Computing sentiment-based summaries with optimal settings...")
            results = client.gather(futures)
        except Exception as e:
            status_text.write(f"Error with futures: {e}")
            status_text.write("Falling back to direct computation...")
            results = dask.compute(*delayed_results)
        
        # Stop progress monitor
        stop_flag[0] = True
        monitor_thread.join(timeout=3)
        
        # Update progress to completion
        progress_bar.progress(1.0)
        
        # Process results efficiently
        all_results = []
        for worker_results in results:
            all_results.extend(worker_results)
        
        # Sort results
        all_results.sort(key=lambda x: x[0])
        
        # Extract positive and negative summaries
        positive_summaries = [result[1] for result in all_results]
        negative_summaries = [result[2] for result in all_results]
        
        # Store results
        final_report['Positive_Summary'] = positive_summaries
        final_report['Negative_Summary'] = negative_summaries
        
        # Report timing
        elapsed_time = time.time() - start_time
        status_text.write(f"✅ Optimized sentiment analysis completed in {elapsed_time:.2f} seconds")
        status_text.write(f"Average time per item: {elapsed_time/len(final_report):.2f} seconds")
        
        # Save results
        final_report.to_csv(DEFAULT_OUTPUT_PATH)
        status_text.write(f"✅ Results saved to {DEFAULT_OUTPUT_PATH}")
        
        # Calculate elapsed time for this phase
        phase_elapsed_time = time.time() - phase_start_time
        formatted_time = str(datetime.timedelta(seconds=int(phase_elapsed_time)))
        status_text.write(f"✅ Total summarization time: {formatted_time}")
        
        return final_report
        
    finally:
        # Clean up
        if client:
            client.close()
        if cluster:
            cluster.close() 