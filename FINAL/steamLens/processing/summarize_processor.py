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
        # Check GPU availability
        import torch
        gpu_available = torch.cuda.is_available()
        if gpu_available:
            device_count = torch.cuda.device_count()
            status_text.write(f"GPU detected: {device_count} device(s) available")
            
            # Adjust memory limit based on GPU availability
            memory_limit = hardware_config['memory_per_worker']
        else:
            status_text.write("No GPU detected, using CPU mode")
            # Increase memory for CPU workers
            memory_limit = hardware_config['memory_per_worker'] * 1.5  # Give more memory for CPU processing
        
        # Create cluster with optimized settings - allow more workers (up to 6) for GPU processing
        # Previously this was limited to 2 for GPU, now allowing more
        worker_count = min(hardware_config['worker_count'], 6 if gpu_available else 4)
        
        cluster = LocalCluster(
            n_workers=worker_count, 
            threads_per_worker=2,
            memory_limit=memory_limit
        )
        client = Client(cluster)
        status_text.write(f"Dask cluster initialized with {worker_count} workers")
        
        # Store client in session state for potential reset
        st.session_state.summarize_client = client
        
        # Immediately display dashboard link for monitoring
        with dashboard_placeholder.container():
            st.success("✅ Dask Summarization Cluster Ready")
            st.markdown(f"**[Open Dask Dashboard]({client.dashboard_link})** (opens in new tab)")
            st.info("👁️ Monitor summarization tasks in real-time with this dashboard")
        
        # Store dashboard link in session state for later reference
        if 'summarize_dashboard_link' not in st.session_state:
            st.session_state.summarize_dashboard_link = client.dashboard_link
        
        # Determine optimal partition sizes for better parallelization
        n_workers = worker_count
        partition_size = max(1, len(final_report) // n_workers)
        
        # Create more balanced partitions
        partitions = []
        for i in range(n_workers):
            start_idx = i * partition_size
            end_idx = min((i + 1) * partition_size, len(final_report))
            if start_idx >= end_idx:
                break  # Skip empty partitions
            partitions.append(dask.delayed(prepare_partition)(start_idx, end_idx, final_report))
            status_text.write(f"Prepared partition {i+1} with {end_idx-start_idx} items")
        
        # Schedule tasks
        status_text.write(f"Scheduling {len(partitions)} optimized partitions for sentiment analysis...")
        delayed_results = []
        for i, partition in enumerate(partitions):
            delayed_result = dask.delayed(process_partition)(partition, i, hardware_config)
            delayed_results.append(delayed_result)
        
        # Start timing
        start_time = time.time()
        
        # Try to submit tasks to cluster with more robust error handling
        results = None
        try:
            # First try with async computation to allow cancellation
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
            status_text.write("Computing sentiment-based summaries with optimal settings...")
            results = client.gather(futures)
            
            # Stop progress monitor
            stop_flag[0] = True
            if monitor_thread.is_alive():
                monitor_thread.join(timeout=3)
        except Exception as e:
            status_text.write(f"Error with futures: {e}")
            status_text.write("Falling back to direct computation...")
            
            # Fallback: directly compute without futures
            results = dask.compute(*delayed_results)
        
        # Update progress to completion
        progress_bar.progress(1.0)
        
        if results is None:
            status_text.write("❌ Failed to generate summaries")
            return None
        
        # Process results efficiently
        all_results = []
        for worker_results in results:
            if worker_results:  # Check if we got valid results
                all_results.extend(worker_results)
        
        if not all_results:
            status_text.write("❌ No valid results generated")
            return None
        
        # Sort results
        all_results.sort(key=lambda x: x[0])
        
        # Extract positive and negative summaries
        indices = [result[0] for result in all_results]
        positive_summaries = [result[1] for result in all_results]
        negative_summaries = [result[2] for result in all_results]
        
        # Create a new DataFrame with the results
        result_df = pd.DataFrame({
            'index': indices,
            'Positive_Summary': positive_summaries,
            'Negative_Summary': negative_summaries
        }).set_index('index')
        
        # Merge with the original DataFrame
        final_report = final_report.join(result_df[['Positive_Summary', 'Negative_Summary']])
        
        # Fill any missing values (for rows that weren't processed)
        final_report['Positive_Summary'] = final_report['Positive_Summary'].fillna("Summary not available")
        final_report['Negative_Summary'] = final_report['Negative_Summary'].fillna("Summary not available")
        
        # Report timing
        elapsed_time = time.time() - start_time
        status_text.write(f"✅ **EXECUTION TIME:** {elapsed_time:.2f} seconds")
        status_text.write(f"✅ Average time per item: {elapsed_time/len(final_report):.4f} seconds")
        
        # Save results
        try:
            final_report.to_csv(DEFAULT_OUTPUT_PATH)
            status_text.write(f"✅ Results saved to {DEFAULT_OUTPUT_PATH}")
        except Exception as e:
            status_text.write(f"⚠️ Failed to save results: {e}")
        
        # Calculate elapsed time for this phase
        phase_elapsed_time = time.time() - phase_start_time
        formatted_time = str(datetime.timedelta(seconds=int(phase_elapsed_time)))
        status_text.write(f"✅ **TOTAL SUMMARIZATION TIME:** {formatted_time}")
        
        return final_report
        
    except Exception as e:
        # Handle any unexpected errors
        status_text.write(f"❌ Error during summarization: {str(e)}")
        import traceback
        status_text.write(f"Error details: {traceback.format_exc()}")
        return None
        
    finally:
        # Clean up
        if client:
            try:
                client.close()
            except:
                pass
        if cluster:
            try:
                cluster.close()
            except:
                pass 