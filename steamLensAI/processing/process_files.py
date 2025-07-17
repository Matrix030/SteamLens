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


def _validate_uploaded_files(uploaded_files: List[Any]) -> bool:
    """Validate that files were uploaded."""
    if not uploaded_files:
        st.warning("Please upload at least one Parquet file to begin processing.")
        return False
    return True


def _create_progress_indicators() -> Tuple[Any, Any, Any]:
    """Create and return progress indicator placeholders."""
    progress_placeholder = st.empty()
    status_placeholder = st.empty()
    dashboard_placeholder = st.empty()
    
    with progress_placeholder.container():
        status_text = st.empty()
    
    return progress_placeholder, status_placeholder, dashboard_placeholder, status_text


def _load_game_themes(themes_file: str, status_text: Any) -> Optional[Dict]:
    """Load the game themes dictionary."""
    try:
        from ..data.data_loader import load_theme_dictionary
        GAME_THEMES = load_theme_dictionary(themes_file)
        if not GAME_THEMES:
            return None
        status_text.write(f"✅ Loaded theme dictionary with {len(GAME_THEMES)} games")
        return GAME_THEMES
    except Exception as e:
        st.error(f"Error loading theme dictionary: {str(e)}")
        return None


def _process_and_validate_files(uploaded_files: List[Any], temp_dir: str, 
                               GAME_THEMES: Dict, status_text: Any) -> Tuple[List, List, Dict]:
    """Process uploaded files and validate against theme dictionary."""
    valid_files = []
    skipped_files = []
    game_name_mapping = {}
    
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
    
    return valid_files, skipped_files, game_name_mapping


def _setup_dask_cluster(resources: Dict, status_text: Any, dashboard_placeholder: Any) -> Tuple[LocalCluster, Client]:
    """Set up Dask cluster with appropriate resources."""
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
    
    # Display dashboard link
    with dashboard_placeholder.container():
        st.markdown(f"**[Open Dask Dashboard]({dashboard_link})** (opens in new tab)")
    
    # Store dashboard link in session state
    if 'process_dashboard_link' not in st.session_state:
        st.session_state.process_dashboard_link = dashboard_link
    
    return cluster, client


def _initialize_embedder(client: Client, status_text: Any) -> str:
    """Initialize and publish the sentence embedder to all workers."""
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
    
    return model_dataset_name


def _read_and_filter_files(valid_files: List[Tuple[str, int]], status_text: Any) -> Tuple[List[dd.DataFrame], int]:
    """Read parquet files and apply initial filtering."""
    all_ddfs = []
    total_rows = 0
    
    for file_path, app_id in valid_files:
        file_name = os.path.basename(file_path)
        status_text.write(f"Processing file: {file_name} (App ID: {app_id})")
        
        rows_here = 0
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
            
            # Get the total number of reviews being processed
            rows_here = ddf.map_partitions(len).compute().sum()
            total_rows += rows_here
            all_ddfs.append(ddf)
            status_text.write(f"Added file to processing queue: {file_name}")
            
        except Exception as e:
            status_text.write(f"⚠️ Error processing file {file_name}: {str(e)}")
    
    return all_ddfs, total_rows


def _apply_topic_assignment(combined_ddf: dd.DataFrame, GAME_THEMES: Dict, 
                           model_dataset_name: str, status_text: Any) -> dd.DataFrame:
    """Apply topic assignment to the combined dataframe."""
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
    
    return combined_ddf.map_partitions(assign_topic_with_context, meta=meta)


def _determine_batch_size(total_app_ids: int) -> int:
    """Determine appropriate batch size based on number of app IDs."""
    if total_app_ids > 1000:  # Very large number of app IDs
        return APP_ID_BATCH_SIZES['very_large']
    elif total_app_ids > 500:  # Medium-large number
        return APP_ID_BATCH_SIZES['large']
    elif total_app_ids > 100:  # Medium number
        return APP_ID_BATCH_SIZES['medium']
    else:  # Smaller number
        return APP_ID_BATCH_SIZES['small']


def _process_batch(batch_app_ids: List[int], ddf_with_topic: dd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Process a batch of app IDs and return aggregated results."""
    # Filter data for this batch of app IDs
    batch_ddf = ddf_with_topic[ddf_with_topic['steam_appid'].isin(batch_app_ids)]
    
    # Aggregate for this batch
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
    
    # Calculate dislikes after computation
    agg_df['dislikes_sum'] = agg_df['review_count'] - agg_df['likes_sum']
    
    # Process positive reviews
    if not positive_reviews_df.empty:
        positive_reviews_df = positive_reviews_df.reset_index().rename(columns={'review': 'Positive_Reviews'})
    else:
        positive_reviews_df = pd.DataFrame()
    
    # Process negative reviews  
    if not negative_reviews_df.empty:
        negative_reviews_df = negative_reviews_df.reset_index().rename(columns={'review': 'Negative_Reviews'})
    else:
        negative_reviews_df = pd.DataFrame()
    
    return agg_df, positive_reviews_df, negative_reviews_df


def _process_app_ids_in_batches(unique_app_ids: List[int], ddf_with_topic: dd.DataFrame, 
                                batch_size: int, status_text: Any) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Process app IDs in batches and return combined results."""
    all_agg_dfs = []
    all_positive_review_dfs = []
    all_negative_review_dfs = []
    
    # Create a progress bar for batch processing
    batch_progress = st.progress(0.0)
    
    # Process in dynamically sized batches
    for i in range(0, len(unique_app_ids), batch_size):
        batch_progress.progress(i / len(unique_app_ids))
        batch_app_ids = unique_app_ids[i:i+batch_size]
        
        agg_df, positive_reviews_df, negative_reviews_df = _process_batch(batch_app_ids, ddf_with_topic)
        
        # Append results
        all_agg_dfs.append(agg_df)
        if not positive_reviews_df.empty:
            all_positive_review_dfs.append(positive_reviews_df)
        if not negative_reviews_df.empty:
            all_negative_review_dfs.append(negative_reviews_df)
        
        status_text.write(f"Processed batch {i//batch_size + 1}/{(len(unique_app_ids) + batch_size - 1)//batch_size}")
    
    # Complete the batch progress
    batch_progress.progress(1.0)
    
    # Combine results
    status_text.write("Combining results...")
    agg_df = pd.concat(all_agg_dfs) if all_agg_dfs else pd.DataFrame()
    positive_reviews_df = pd.concat(all_positive_review_dfs) if all_positive_review_dfs else pd.DataFrame()
    negative_reviews_df = pd.concat(all_negative_review_dfs) if all_negative_review_dfs else pd.DataFrame()
    
    return agg_df, positive_reviews_df, negative_reviews_df


def _merge_results(agg_df: pd.DataFrame, positive_reviews_df: pd.DataFrame, 
                  negative_reviews_df: pd.DataFrame) -> pd.DataFrame:
    """Merge aggregation results with sentiment-specific reviews."""
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
    
    return report_df


def _get_theme_name_safely(row: pd.Series, themes_dict: Dict) -> str:
    """Helper function to get theme name safely."""
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


def _build_final_report(report_df: pd.DataFrame, GAME_THEMES: Dict) -> pd.DataFrame:
    """Build the final report with all required columns."""
    if report_df.empty:
        # Return empty dataframe with correct columns
        return pd.DataFrame(columns=[
            'steam_appid', 'Theme', '#Reviews', 'Positive', 'Negative',
            'LikeRatio', 'DislikeRatio', 'Positive_Reviews', 'Negative_Reviews'
        ])
    
    # Apply theme names
    report_df['Theme'] = report_df.apply(lambda row: _get_theme_name_safely(row, GAME_THEMES), axis=1)
    
    # Vectorized calculations for ratios
    report_df['#Reviews'] = report_df['review_count'].astype(int)
    report_df['Positive'] = report_df['likes_sum'].astype(int)
    report_df['Negative'] = report_df['dislikes_sum'].astype(int)
    
    # Calculate ratios
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
        'Positive_Reviews',
        'Negative_Reviews'
    ]].copy()
    
    final_report['steam_appid'] = final_report['steam_appid'].astype(int)
    
    # Ensure review columns are lists
    final_report['Positive_Reviews'] = final_report['Positive_Reviews'].apply(
        lambda x: x if isinstance(x, list) else []
    )
    final_report['Negative_Reviews'] = final_report['Negative_Reviews'].apply(
        lambda x: x if isinstance(x, list) else []
    )
    
    return final_report


def process_uploaded_files(uploaded_files: List[Any], themes_file: str = "game_themes.json") -> Optional[Dict[str, Any]]:
    """Main function to process uploaded parquet files."""
    # Start phase timer
    phase_start_time = time.time()
    
    # Validate files
    if not _validate_uploaded_files(uploaded_files):
        return None
    
    # Create progress indicators
    progress_placeholder, status_placeholder, dashboard_placeholder, status_text = _create_progress_indicators()
    
    # Load theme dictionary
    GAME_THEMES = _load_game_themes(themes_file, status_text)
    if not GAME_THEMES:
        return None
    
    # Create a temporary directory to store uploaded files
    with tempfile.TemporaryDirectory() as temp_dir:
        # Process and validate files
        valid_files, skipped_files, game_name_mapping = _process_and_validate_files(
            uploaded_files, temp_dir, GAME_THEMES, status_text
        )
        
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
            # Setup Dask cluster
            cluster, client = _setup_dask_cluster(resources, status_text, dashboard_placeholder)
            dashboard_link = client.dashboard_link
            
            # Initialize embedder
            model_dataset_name = _initialize_embedder(client, status_text)
            
            # Read and filter files
            all_ddfs, total_rows = _read_and_filter_files(valid_files, status_text)
            
            # Store total rows in session state
            st.session_state["total_rows"] = total_rows
            if st.session_state.get("total_rows") is not None:
                st.sidebar.markdown("---")
                st.sidebar.markdown(f"**Total Reviews:** {st.session_state['total_rows']}")
            
            # Combine all dataframes
            if not all_ddfs:
                st.error("No data to process after filtering. Please check your files.")
                return None
            
            status_text.write("Combining all valid data for processing...")
            combined_ddf = dd.concat(all_ddfs)
            
            # Apply topic assignment
            ddf_with_topic = _apply_topic_assignment(combined_ddf, GAME_THEMES, model_dataset_name, status_text)
            
            # Get unique app IDs
            unique_app_ids = combined_ddf['steam_appid'].unique().compute()
            total_app_ids = len(unique_app_ids)
            
            # Determine batch size
            batch_size = _determine_batch_size(total_app_ids)
            status_text.write(f"Processing {total_app_ids} unique app IDs with batch size {batch_size}")
            
            # Process app IDs in batches
            agg_df, positive_reviews_df, negative_reviews_df = _process_app_ids_in_batches(
                unique_app_ids, ddf_with_topic, batch_size, status_text
            )
            
            # Check if we have any data
            if agg_df.empty:
                st.error("No data after processing. Please check your files and filters.")
                return None
            
            # Merge results
            report_df = _merge_results(agg_df, positive_reviews_df, negative_reviews_df)
            
            # Build final report
            status_text.write("Building final report with sentiment analysis...")
            final_report = _build_final_report(report_df, GAME_THEMES)
            
            # Save intermediate results
            final_report.to_csv(DEFAULT_INTERIM_PATH, index=False)
            status_text.write(f"✅ Saved sentiment report to {DEFAULT_INTERIM_PATH}")
            
            # Calculate elapsed time
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
                'game_name_mapping': game_name_mapping
            }
            
        finally:
            # Ensure resources are cleaned up properly
            if client:
                client.close()
            if cluster:
                cluster.close()