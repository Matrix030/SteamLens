#!/usr/bin/env python3



import os
import time
import numpy as np
import pandas as pd
import tempfile
import datetime
import streamlit as st
import dask.dataframe as dd
from typing import Dict, List, Tuple, Any, Optional
from dask.distributed import Client, LocalCluster
from sentence_transformers import SentenceTransformer

from ..config.app_config import (SENTENCE_TRANSFORMER_MODEL, PARQUET_COLUMNS, 
                                DEFAULT_LANGUAGE, APP_ID_BATCH_SIZES, DEFAULT_INTERIM_PATH)
from ..data.data_loader import extract_appid_and_name_from_parquet, determine_blocksize
from ..utils.system_utils import get_system_resources
from .topic_assignment import assign_topic

def process_uploaded_files(uploaded_files: List[Any], themes_file: str = "game_themes.json") -> Optional[Dict[str, Any]]:
    
    # Start phase timer for more detailed timing
    phase_start_time = time.time()
    
    if not uploaded_files:
        st.warning("Please upload at least one Parquet file to begin processing.")
        return None
    
    # Create progress indicators
    progress_placeholder = st.empty()
    status_placeholder = st.empty()
    dashboard_placeholder = st.empty()  # Add placeholder for dashboard link
    
    with progress_placeholder.container():
        # progress_bar = st.progress(0.0)
        status_text = st.empty()
    
    # Load theme dictionary
    try:
        from ..data.data_loader import load_theme_dictionary
        GAME_THEMES = load_theme_dictionary(themes_file)
        if not GAME_THEMES:
            return None
        status_text.write(f"✅ Loaded theme dictionary with {len(GAME_THEMES)} games")
    except Exception as e:
        st.error(f"Error loading theme dictionary: {str(e)}")
        return None
    
    # Create a temporary directory to store uploaded files
    with tempfile.TemporaryDirectory() as temp_dir:
        valid_files = []
        skipped_files = []
        game_name_mapping = {}  # Store mapping of app_id to game_name
        
        # Check and save valid files
        for uploaded_file in uploaded_files:
            file_path = os.path.join(temp_dir, uploaded_file.name)
            
            # Save the uploaded file
            with open(file_path, "wb") as f:
                f.write(uploaded_file.getbuffer())
            
            # Extract app ID and game name and check if it's in the theme dictionary
            app_id, game_name = extract_appid_and_name_from_parquet(file_path)
            
            if app_id and app_id in GAME_THEMES:
                valid_files.append((file_path, app_id))
                if game_name:
                    game_name_mapping[app_id] = game_name
                status_text.write(f"✅ File '{uploaded_file.name}' has app ID {app_id} - Processing")
            else:
                skipped_files.append((uploaded_file.name, app_id))
                status_text.write(f"⚠️ File '{uploaded_file.name}' has app ID {app_id} - Skipping (not in theme dictionary)")
        
        # Check if we have any valid files
        if not valid_files:
            st.error("No valid files to process. All uploaded files' app IDs were not found in the theme dictionary.")
            return None
        
        status_text.write(f"Starting processing with {len(valid_files)} valid files...")
        
        # Dynamic resource allocation
        resources = get_system_resources()
        status_text.write(f"System has {resources['total_memory']:.1f}GB memory and {resources['worker_count']} CPU cores")
        status_text.write(f"Allocating {resources['worker_count']} workers with {resources['memory_per_worker']}GB each")

        # Use try/finally to ensure proper cleanup of Dask resources
        cluster = None
        client = None
        
        try:
            # Start a local Dask cluster with dynamically determined resources
            cluster = LocalCluster(
                n_workers=resources['worker_count'],
                threads_per_worker=4,
                memory_limit=f"{resources['memory_per_worker']}GB"
            )
            client = Client(cluster)
            dashboard_link = client.dashboard_link
            status_text.write(f"Dask dashboard initialized")
            
            # Store client in session state for potential reset
            st.session_state.process_client = client
            
            # Immediately display dashboard link for monitoring
            with dashboard_placeholder.container():
                st.markdown(f"**[Open Dask Dashboard]({dashboard_link})** (opens in new tab)")
            
            # Store dashboard link in session state for later reference
            if 'process_dashboard_link' not in st.session_state:
                st.session_state.process_dashboard_link = dashboard_link
            
            # Initialize SBERT embedder once and share with all workers
            status_text.write(f"Initializing sentence embedder: {SENTENCE_TRANSFORMER_MODEL}")
            # Set device to CPU explicitly to avoid CUDA tensor serialization issues
            embedder = SentenceTransformer(SENTENCE_TRANSFORMER_MODEL, device='cpu')
            
            # Ensure all model parameters are on CPU before sharing
            for param in embedder.parameters():
                if hasattr(param, 'data') and hasattr(param.data, 'cpu'):
                    param.data = param.data.cpu()
                    
            # Create a unique name for the model dataset
            model_dataset_name = f"embedder_dataset_{int(time.time())}"
            
            # Publish the embedder to all workers
            client.publish_dataset(embedder, name=model_dataset_name)
            status_text.write(f"✅ Sentence embedder published to all workers with dataset name: {model_dataset_name}")
            
            # Process each valid file
            all_ddfs = []
            
            for file_path, app_id in valid_files:
                file_name = os.path.basename(file_path)
                status_text.write(f"Processing file: {file_name} (App ID: {app_id})")
                
                # Determine blocksize based on file size
                file_size = os.path.getsize(file_path) / (1024**3)  # in GB
                blocksize = determine_blocksize(file_size)
                
                status_text.write(f"Using blocksize: {blocksize} for {file_size:.2f}GB file")
                
                # Read the parquet file with Dask
                try:
                    ddf = dd.read_parquet(
                        file_path,
                        columns=PARQUET_COLUMNS,
                        blocksize=blocksize
                    )
                    
                    # Filter & Clean Data
                    ddf = ddf[ddf['review_language'] == DEFAULT_LANGUAGE]
                    ddf = ddf.dropna(subset=['review'])
                    
                    # Only include matching app_id
                    ddf = ddf[ddf['steam_appid'] == app_id]
                    
                    all_ddfs.append(ddf)
                    status_text.write(f"Added file to processing queue: {file_name}")
                    
                except Exception as e:
                    status_text.write(f"⚠️ Error processing file {file_name}: {str(e)}")
            
            # Combine all dataframes
            if not all_ddfs:
                st.error("No data to process after filtering. Please check your files.")
                return None
            
            status_text.write("Combining all valid data for processing...")
            combined_ddf = dd.concat(all_ddfs)
            
            # Apply topic assignment
            status_text.write("Assigning topics to reviews...")
            meta = combined_ddf._meta.assign(topic_id=np.int64())
            
            # Create a function that includes GAME_THEMES and the model_dataset_name
            def assign_topic_with_context(df_partition):
                from dask.distributed import get_worker
                # Get the worker's client to access the dataset
                worker = get_worker()
                # Get the model from the published dataset
                embedder = worker.client.get_dataset(model_dataset_name)
                # Now process with the actual model instance
                return assign_topic(df_partition, GAME_THEMES, embedder)
            
            ddf_with_topic = combined_ddf.map_partitions(assign_topic_with_context, meta=meta)
            
            # Get unique app IDs
            unique_app_ids = combined_ddf['steam_appid'].unique().compute()
            total_app_ids = len(unique_app_ids)
            
            # Dynamically determine batch size based on number of app IDs and memory
            if total_app_ids > 1000:  # Very large number of app IDs
                batch_size = APP_ID_BATCH_SIZES['very_large']
            elif total_app_ids > 500:  # Medium-large number
                batch_size = APP_ID_BATCH_SIZES['large']
            elif total_app_ids > 100:  # Medium number
                batch_size = APP_ID_BATCH_SIZES['medium']
            else:  # Smaller number
                batch_size = APP_ID_BATCH_SIZES['small']
            
            status_text.write(f"Processing {total_app_ids} unique app IDs with batch size {batch_size}")
            
            # Initialize empty dataframes for results
            all_agg_dfs = []
            all_positive_review_dfs = []
            all_negative_review_dfs = []
            
            # Create a progress bar for batch processing
            batch_progress = st.progress(0.0)
            
            # Process in dynamically sized batches
            for i in range(0, len(unique_app_ids), batch_size):
                batch_progress.progress(i / len(unique_app_ids))
                batch_app_ids = unique_app_ids[i:i+batch_size]
                
                # Filter data for this batch of app IDs
                batch_ddf = ddf_with_topic[ddf_with_topic['steam_appid'].isin(batch_app_ids)]
                
                # Aggregate for this batch - Fixed version without lambda
                agg = batch_ddf.groupby(['steam_appid', 'topic_id']).agg(
                    review_count=('review', 'count'),
                    likes_sum=('voted_up', 'sum')
                )
                
                # Collect positive reviews for this batch
                positive_reviews_series = batch_ddf[batch_ddf['voted_up']].groupby(['steam_appid', 'topic_id'])['review'] \
                    .apply(lambda x: list(x), meta=('review', object))
                
                # Collect negative reviews for this batch
                negative_reviews_series = batch_ddf[~batch_ddf['voted_up']].groupby(['steam_appid', 'topic_id'])['review'] \
                    .apply(lambda x: list(x), meta=('review', object))
                
                # Compute all in parallel
                agg_df, positive_reviews_df, negative_reviews_df = dd.compute(
                    agg, positive_reviews_series, negative_reviews_series
                )
                
                # Convert to DataFrames
                agg_df = agg_df.reset_index()
                
                # Calculate dislikes after computation (total reviews - likes)
                agg_df['dislikes_sum'] = agg_df['review_count'] - agg_df['likes_sum']
                
                # Process positive reviews
                if not positive_reviews_df.empty:
                    positive_reviews_df = positive_reviews_df.reset_index().rename(columns={'review': 'Positive_Reviews'})
                    all_positive_review_dfs.append(positive_reviews_df)
                
                # Process negative reviews
                if not negative_reviews_df.empty:
                    negative_reviews_df = negative_reviews_df.reset_index().rename(columns={'review': 'Negative_Reviews'})
                    all_negative_review_dfs.append(negative_reviews_df)
                
                # Append to aggregation results
                all_agg_dfs.append(agg_df)
                
                status_text.write(f"Processed batch {i//batch_size + 1}/{(len(unique_app_ids) + batch_size - 1)//batch_size}")
            
            # Complete the batch progress
            batch_progress.progress(1.0)
            
            # Combine results
            status_text.write("Combining results...")
            agg_df = pd.concat(all_agg_dfs) if all_agg_dfs else pd.DataFrame()
            
            # Check if we have any data
            if agg_df.empty:
                st.error("No data after processing. Please check your files and filters.")
                return None
            
            # Process positive reviews
            positive_reviews_df = pd.concat(all_positive_review_dfs) if all_positive_review_dfs else pd.DataFrame()
            
            # Process negative reviews
            negative_reviews_df = pd.concat(all_negative_review_dfs) if all_negative_review_dfs else pd.DataFrame()
            
            # Merge counts and sentiment-specific reviews
            # First, merge aggregation with positive reviews
            if not positive_reviews_df.empty:
                report_df = pd.merge(
                    agg_df,
                    positive_reviews_df,
                    on=['steam_appid', 'topic_id'],
                    how='left'
                )
            else:
                report_df = agg_df.copy()
                report_df['Positive_Reviews'] = None
            
            # Then, merge with negative reviews
            if not negative_reviews_df.empty:
                report_df = pd.merge(
                    report_df,
                    negative_reviews_df,
                    on=['steam_appid', 'topic_id'],
                    how='left'
                )
            else:
                report_df['Negative_Reviews'] = None
            
            # Build the final output structure
            status_text.write("Building final report with sentiment analysis...")

            # Helper function to get theme name safely
            def get_theme_name_safely(row, themes_dict):
                appid = int(row['steam_appid'])
                tid = int(row['topic_id'])
                if appid in themes_dict:
                    theme_keys = list(themes_dict[appid].keys())
                    if 0 <= tid < len(theme_keys):
                        return theme_keys[tid]
                    else:
                        return f"Unknown Theme {tid} (Index out of bounds)"
                else:
                    return f"Unknown Theme {tid} (AppID not in themes)"

            if not report_df.empty:
                # Apply the helper function to create the 'Theme' column
                report_df['Theme'] = report_df.apply(lambda row: get_theme_name_safely(row, GAME_THEMES), axis=1)

                # Vectorized calculations for ratios
                report_df['#Reviews'] = report_df['review_count'].astype(int)
                report_df['Positive'] = report_df['likes_sum'].astype(int)
                report_df['Negative'] = report_df['dislikes_sum'].astype(int)

                # Ensure '#Reviews' is not zero to avoid division by zero errors
                report_df['LikeRatio'] = '0.0%'
                report_df.loc[report_df['#Reviews'] > 0, 'LikeRatio'] = \
                    (report_df['Positive'] / report_df['#Reviews'] * 100).round(1).astype(str) + '%'
                
                report_df['DislikeRatio'] = '0.0%'
                report_df.loc[report_df['#Reviews'] > 0, 'DislikeRatio'] = \
                    (report_df['Negative'] / report_df['#Reviews'] * 100).round(1).astype(str) + '%'

                # Select and rename columns for the final report
                final_report = report_df[[
                    'steam_appid',
                    'Theme',
                    '#Reviews',
                    'Positive',
                    'Negative',
                    'LikeRatio',
                    'DislikeRatio',
                    'Positive_Reviews', # Already exists from merge, ensure it's correctly populated
                    'Negative_Reviews'  # Already exists from merge, ensure it's correctly populated
                ]].copy()
                final_report['steam_appid'] = final_report['steam_appid'].astype(int)
            else:
                # If report_df is empty, create an empty final_report with correct columns
                final_report = pd.DataFrame(columns=[
                    'steam_appid', 'Theme', '#Reviews', 'Positive', 'Negative',
                    'LikeRatio', 'DislikeRatio', 'Positive_Reviews', 'Negative_Reviews'
                ])

            # Ensure Positive_Reviews and Negative_Reviews are lists, fillna with empty list if they are None
            # This was previously handled in the loop, ensure it here for consistency
            if 'Positive_Reviews' in final_report.columns:
                final_report['Positive_Reviews'] = final_report['Positive_Reviews'].apply(
                    lambda x: x if isinstance(x, list) else []
                )
            else:
                final_report['Positive_Reviews'] = pd.Series([[] for _ in range(len(final_report))], index=final_report.index)
                
            if 'Negative_Reviews' in final_report.columns:
                final_report['Negative_Reviews'] = final_report['Negative_Reviews'].apply(
                    lambda x: x if isinstance(x, list) else []
                )
            else:
                final_report['Negative_Reviews'] = pd.Series([[] for _ in range(len(final_report))], index=final_report.index)

            # Save intermediate results to avoid recomputation if summarization fails
            final_report.to_csv(DEFAULT_INTERIM_PATH, index=False)
            status_text.write(f"✅ Saved sentiment report to {DEFAULT_INTERIM_PATH}")
            
            # Complete the progress bar
            # progress_bar.progress(1.0)
            
            # Calculate elapsed time for this phase
            phase_elapsed_time = time.time() - phase_start_time
            formatted_time = str(datetime.timedelta(seconds=int(phase_elapsed_time)))
            
            status_text.write(f"✅ Data processing complete! Total processing time: {formatted_time}")
            
            # Return results
            return {
                'final_report': final_report,
                'valid_files': valid_files,
                'skipped_files': skipped_files,
                'dashboard_link': dashboard_link,
                'processing_time': phase_elapsed_time,
                'game_name_mapping': game_name_mapping  # Add the game name mapping to the return value
            }
            
        finally:
            # Ensure resources are cleaned up properly
            if client:
                client.close()
            if cluster:
                cluster.close() 