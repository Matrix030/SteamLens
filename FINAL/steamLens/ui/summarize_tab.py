#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
summarize_tab.py - UI for the Summarize tab
Handles summarization of processed data
"""

import time
import streamlit as st
from typing import Dict, Any

from ..processing.summarize_processor import summarize_report

def render_summarize_tab() -> None:
    """Render the Summarize tab UI
    
    Displays UI for generating sentiment summaries from processed data
    """
    st.header("Generate Sentiment Summaries")
    st.write("""
    This step uses GPU-accelerated summarization to generate separate summaries for positive and negative reviews.
    This gives you better insights into what players love and hate about each theme.
    """)
    
    # Check if we have a result from previous step
    if 'result' in st.session_state and st.session_state.result:
        # Start summarization button
        if st.button("🚀 Start Summarization", key="summarize_button"):
            with st.spinner("Initializing Dask cluster..."):
                # Record start time for performance comparison
                start_time = time.time()
                st.session_state.timing_data['summarize_start_time'] = start_time
                
                # Inform user that summarization is starting
                st.info("Dask cluster is being initialized. Summarization will start shortly...")
                st.info("The dashboard link will appear below once the cluster is ready.")
            
            # Run summarization (note: moved outside the spinner to allow dashboard to show)
            summarized_report = summarize_report(st.session_state.result['final_report'])
            
            # Calculate elapsed time
            elapsed_time = time.time() - start_time
            st.session_state.timing_data['summarize_end_time'] = time.time()
            
            if summarized_report is not None:
                st.session_state.summarized_report = summarized_report
                st.success(f"✅ Sentiment summarization completed in {elapsed_time:.2f} seconds!")
                
                # Show sample of summarized data
                with st.expander("Show sample of sentiment summaries"):
                    sample_columns = ['steam_appid', 'Theme', 'Positive_Summary', 'Negative_Summary']
                    st.dataframe(summarized_report[sample_columns].head(5))
                
                # Switch to results tab
                st.info("👉 Go to the 'Results' tab to view the complete sentiment analysis")
    else:
        st.info("Please complete the 'Upload & Process' step first") 