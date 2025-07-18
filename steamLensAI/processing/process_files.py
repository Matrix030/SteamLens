#!/usr/bin/env python3

import os
import gc
import time
import numpy as np
import pandas as pd
import tempfile
import datetime
import streamlit as st
import dask
import dask.dataframe as dd
from typing import Dict, List, Tuple, Any, Optional
from dask.distributed import Client, LocalCluster, as_completed
from sentence_transformers import SentenceTransformer
from pathlib import Path
import shutil
import torch
from dask import delayed

from ..config.app_config import (SENTENCE_TRANSFORMER_MODEL, PARQUET_COLUMNS, 
                                DEFAULT_LANGUAGE, DEFAULT_INTERIM_PATH, PROCESSING_CONFIG)
from ..data.data_loader import extract_appid_and_name_from_parquet, determine_blocksize
from ..utils.system_utils import get_system_resources
from .topic_assignment import assign_topic

# Try to import monitoring utilities, but make them optional
try:
    from ..utils.dask_monitor import DaskMonitor, create_progress_tracker, update_progress_tracker, display_performance_summary
    MONITORING_AVAILABLE = True
except ImportError:
    MONITORING_AVAILABLE = False
    # Create dummy functions if monitoring is not available
    class DaskMonitor:
        def __init__(self, client):
            self.client = client
            self.start_time = time.time()
        
        def log_task_completion(self, *args, **kwargs):
            pass
        
        def display_cluster_metrics(self, *args, **kwargs):
            pass
        
        def get_performance_summary(self):
            return {}
        
        def estimate_completion_time(self, completed, total):
            return None
    
    def create_progress_tracker(total_chunks, placeholder):
        progress_bar = placeholder.progress(0.0)
        status_text = placeholder.empty()
        return {
            'progress_bar': progress_bar,
            'status_text': status_text,
            'chunks_metric': None,
            'rows_metric': None,
            'time_metric': None,
            'eta_metric': None
        }
    
    def update_progress_tracker(tracker, completed, total_chunks, total_rows, monitor):
        progress = completed / total_chunks if total_chunks > 0 else 0
        tracker['progress_bar'].progress(progress)
        tracker['status_text'].write(f"Processing chunk {completed}/{total_chunks} ({total_rows:,} rows)")
    
    def display_performance_summary(monitor, container):
        container.success("Processing complete!")

# Add the monitoring integration to the main processing function
def process_uploaded_files(uploaded_files: List[Any], themes_file: str = "game_themes.json") -> Optional[Dict[str, Any]]:
    """Process uploaded files using Dask parallel chunk processing with monitoring"""
    
    # Start phase timer
    phase_start_time = time.time()
    
    if not uploaded_files:
        st.warning("Please upload at least one Parquet file to begin processing.")
        return None
    
    # Create layout for progress monitoring
    col1, col2 = st.columns([3, 1])
    
    with col1:
        progress_placeholder = st.empty()
        status_placeholder = st.empty()
    
    with col2:
        dashboard_placeholder = st.empty()
        metrics_placeholder = st.empty()
    
    # Initial status
    with status_placeholder.container():
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
    
    # Create temporary storage
    temp_paths = create_temp_storage()
    status_text.write("✅ Created temporary storage for chunk processing")
    
    # Initialize Dask cluster
    cluster = None
    client = None
    
    try:
        # Get processing configuration
        config = PROCESSING_CONFIG
        
        # Display system configuration
        with metrics_placeholder.container():
            from ..config.app_config import display_system_config
            st.info(display_system_config())
        
        # Create LocalCluster optimized for the system
        status_text.write(f"Starting Dask cluster with {config['n_workers']} workers...")
        
        cluster = LocalCluster(
            n_workers=config['n_workers'],
            threads_per_worker=config['threads_per_worker'],
            memory_limit=config['memory_per_worker'],
            processes=True,  # Use processes for better memory isolation
            dashboard_address=':8787'  # Standard dashboard port
        )
        client = Client(cluster)
        
        # Initialize Dask monitor
        monitor = DaskMonitor(client)
        
        # Display dashboard link
        dashboard_link = client.dashboard_link
        with dashboard_placeholder.container():
            st.success("✅ Dask Cluster Ready")
            st.markdown(f"**[Open Dashboard]({dashboard_link})**")
            st.caption("Real-time monitoring")
        
        # Initialize embedder and publish to workers
        status_text.write(f"Initializing sentence embedder: {SENTENCE_TRANSFORMER_MODEL}")
        
        # Check if GPU is available
        device = 'cuda' if torch.cuda.is_available() and config['use_gpu'] else 'cpu'
        embedder = SentenceTransformer(SENTENCE_TRANSFORMER_MODEL, device=device)
        
        # Ensure model is on CPU before publishing (for serialization)
        embedder.to('cpu')
        for param in embedder.parameters():
            if hasattr(param, 'data'):
                param.data = param.data.cpu()
        
        # Publish embedder to all workers
        model_dataset_name = f"embedder_{int(time.time())}"
        client.publish_dataset(embedder, name=model_dataset_name)
        status_text.write(f"✅ Published embedder to {config['n_workers']} workers")
        
        # Process uploaded files
        with tempfile.TemporaryDirectory() as upload_temp_dir:
            valid_files = []
            skipped_files = []
            game_name_mapping = {}
            
            # Validate files
            for uploaded_file in uploaded_files:
                file_path = os.path.join(upload_temp_dir, uploaded_file.name)
                
                with open(file_path, "wb") as f:
                    f.write(uploaded_file.getbuffer())
                
                app_id, game_name = extract_appid_and_name_from_parquet(file_path)
                
                if app_id and app_id in GAME_THEMES:
                    valid_files.append((file_path, app_id))
                    if game_name:
                        game_name_mapping[app_id] = game_name
                    status_text.write(f"✅ File '{uploaded_file.name}' has app ID {app_id} - Processing")
                else:
                    skipped_files.append((uploaded_file.name, app_id))
                    status_text.write(f"⚠️ File '{uploaded_file.name}' has app ID {app_id} - Skipping")
            
            if not valid_files:
                st.error("No valid files to process.")
                return None
            
            # Create all chunks from all files
            status_text.write("Creating processing chunks...")
            all_chunks = []
            chunk_id_counter = 0
            
            for file_path, app_id in valid_files:
                file_chunks = create_file_chunks(file_path, app_id, config['chunk_size'])
                
                # Add unique chunk IDs
                for start, end, fp, aid in file_chunks:
                    all_chunks.append((start, end, fp, aid, f"chunk_{chunk_id_counter}"))
                    chunk_id_counter += 1
            
            total_chunks = len(all_chunks)
            status_text.write(f"Created {total_chunks} chunks for parallel processing")
            
            # Create progress tracker
            tracker = create_progress_tracker(total_chunks, progress_placeholder)
            
            # Create delayed tasks
            delayed_tasks = []
            task_times = {}  # Track task submission times
            
            for start, end, file_path, app_id, chunk_id in all_chunks:
                # Load chunk data
                chunk_data = load_chunk_data((start, end, file_path, app_id))
                
                # Create delayed task
                task = process_chunk_delayed(
                    chunk_data, chunk_id, app_id, 
                    GAME_THEMES, model_dataset_name, temp_paths
                )
                delayed_tasks.append(task)
                task_times[chunk_id] = time.time()
            
            # Submit all tasks to cluster
            tracker['status_text'].write(f"Submitting {len(delayed_tasks)} tasks to Dask cluster...")
            futures = client.compute(delayed_tasks)
            
            # Map futures to chunk IDs for tracking
            future_to_chunk = {futures[i]: all_chunks[i][4] for i in range(len(futures))}
            
            # Track progress
            completed = 0
            total_rows_processed = 0
            
            # Process results as they complete
            for future in as_completed(futures):
                chunk_id = future_to_chunk[future]
                task_start_time = task_times.get(chunk_id, time.time())
                
                # Get result
                result = future.result()
                completed += 1
                total_rows_processed += result['processed_rows']
                
                # Log task completion
                task_duration = time.time() - task_start_time
                monitor.log_task_completion(chunk_id, task_duration, result['processed_rows'])
                
                # Update progress tracker
                update_progress_tracker(tracker, completed, total_chunks, 
                                      total_rows_processed, monitor)
                
                # Update cluster metrics periodically
                if completed % 5 == 0:
                    monitor.display_cluster_metrics(metrics_placeholder)
                
                # Periodic memory cleanup
                if completed % 10 == 0:
                    gc.collect()
            
            # Store total rows in session state
            st.session_state["total_rows"] = total_rows_processed
            
            # Final update
            update_progress_tracker(tracker, completed, total_chunks, 
                                  total_rows_processed, monitor)
            
            # Display final cluster metrics
            monitor.display_cluster_metrics(metrics_placeholder)
            
            # Aggregate all results
            tracker['status_text'].write("Aggregating chunk results...")
            final_report = aggregate_temp_results(temp_paths, GAME_THEMES, game_name_mapping)
            
            if final_report.empty:
                st.error("No data after processing.")
                return None
            
            # Save final report
            final_report.to_csv(DEFAULT_INTERIM_PATH, index=False)
            tracker['status_text'].write(f"✅ Saved sentiment report to {DEFAULT_INTERIM_PATH}")
            
            # Display performance summary
            display_performance_summary(monitor, status_placeholder)
            
            # Calculate elapsed time
            phase_elapsed_time = time.time() - phase_start_time
            formatted_time = str(datetime.timedelta(seconds=int(phase_elapsed_time)))
            
            # Final success message
            st.success(
                f"✅ **Data Processing Complete!**\n\n"
                f"- Total time: {formatted_time}\n"
                f"- Processed: {total_rows_processed:,} reviews\n"
                f"- Parallel chunks: {total_chunks}\n"
                f"- Workers used: {config['n_workers']}"
            )
            
            # Return results
            return {
                'final_report': final_report,
                'valid_files': valid_files,
                'skipped_files': skipped_files,
                'processing_time': phase_elapsed_time,
                'game_name_mapping': game_name_mapping,
                'total_rows': total_rows_processed,
                'dashboard_link': dashboard_link,
                'performance_summary': monitor.get_performance_summary()
            }
            
    except Exception as e:
        st.error(f"Error during processing: {str(e)}")
        import traceback
        st.error(f"Traceback: {traceback.format_exc()}")
        return None
        
    finally:
        # Clean up
        cleanup_temp_storage(temp_paths)
        if client:
            client.close()
        if cluster:
            cluster.close()


# Helper functions remain the same
def create_temp_storage() -> Dict[str, str]:
    """Create temporary directory structure for intermediate results"""
    TEMP_DIR_PREFIX = "steamlens_tem_dir_"
    temp_base = tempfile.mkdtemp(prefix=TEMP_DIR_PREFIX)
    
    paths = {
        'base': temp_base,
        'aggregations': os.path.join(temp_base, 'aggregations'),
        'positive_reviews': os.path.join(temp_base, 'positive_reviews'),
        'negative_reviews': os.path.join(temp_base, 'negative_reviews'),
        'metadata': os.path.join(temp_base, 'metadata')
    }
    
    for path in paths.values():
        if path != temp_base:
            os.makedirs(path, exist_ok=True)
    
    return paths


def cleanup_temp_storage(temp_paths: Dict[str, str]) -> None:
    """Clean up temporary storage"""
    try:
        shutil.rmtree(temp_paths['base'])
    except Exception as e:
        st.warning(f"Could not clean up temporary files: {str(e)}")


@dask.delayed
def process_chunk_delayed(chunk_df: pd.DataFrame, chunk_id: str, app_id: int, 
                         GAME_THEMES: Dict, model_dataset_name: str,
                         temp_paths: Dict[str, str]) -> Dict[str, Any]:
    """Process a single chunk using Dask delayed - runs on worker"""
    from dask.distributed import get_worker
    
    # Get the embedder from the published dataset
    worker = get_worker()
    embedder = worker.client.get_dataset(model_dataset_name)
    
    # Assign topics to the chunk
    chunk_with_topics = assign_topic(chunk_df, GAME_THEMES, embedder)
    
    # Prepare aggregation data
    agg_data = []
    topic_ids = chunk_with_topics['topic_id'].unique()
    
    for topic_id in topic_ids:
        topic_df = chunk_with_topics[chunk_with_topics['topic_id'] == topic_id]
        
        # Basic metrics
        review_count = len(topic_df)
        likes_sum = topic_df['voted_up'].sum()
        dislikes_sum = review_count - likes_sum
        
        agg_data.append({
            'steam_appid': app_id,
            'topic_id': topic_id,
            'review_count': review_count,
            'likes_sum': likes_sum,
            'dislikes_sum': dislikes_sum
        })
        
        # Save positive reviews
        positive_reviews = topic_df[topic_df['voted_up']]['review'].tolist()
        if positive_reviews:
            pos_file = os.path.join(temp_paths['positive_reviews'], 
                                    f"chunk_{chunk_id}_app_{app_id}_topic_{topic_id}.parquet")
            pd.DataFrame({
                'steam_appid': app_id,
                'topic_id': topic_id,
                'reviews': [positive_reviews]
            }).to_parquet(pos_file, compression='snappy')
        
        # Save negative reviews
        negative_reviews = topic_df[~topic_df['voted_up']]['review'].tolist()
        if negative_reviews:
            neg_file = os.path.join(temp_paths['negative_reviews'], 
                                    f"chunk_{chunk_id}_app_{app_id}_topic_{topic_id}.parquet")
            pd.DataFrame({
                'steam_appid': app_id,
                'topic_id': topic_id,
                'reviews': [negative_reviews]
            }).to_parquet(neg_file, compression='snappy')
    
    # Save aggregation data
    if agg_data:
        agg_file = os.path.join(temp_paths['aggregations'], f"chunk_{chunk_id}_app_{app_id}.parquet")
        pd.DataFrame(agg_data).to_parquet(agg_file, compression='snappy')
    
    # Clear memory
    del chunk_with_topics
    gc.collect()
    
    return {
        'chunk_id': chunk_id,
        'processed_rows': len(chunk_df),
        'topics_found': len(topic_ids),
        'app_id': app_id
    }


# Include other helper functions from the original refactored version
def create_file_chunks(file_path: str, app_id: int, chunk_size: int) -> List[Tuple[int, int, str, int]]:
    """Create chunk boundaries for a file without loading all data"""
    # Read just to get row count
    df_info = pd.read_parquet(file_path, columns=['steam_appid', 'review_language'])
    df_filtered = df_info[(df_info['review_language'] == DEFAULT_LANGUAGE) & 
                         (df_info['steam_appid'] == app_id)]
    total_rows = len(df_filtered)
    del df_info, df_filtered
    gc.collect()
    
    # Create chunk boundaries
    chunks = []
    for start in range(0, total_rows, chunk_size):
        end = min(start + chunk_size, total_rows)
        chunks.append((start, end, file_path, app_id))
    
    return chunks


def load_chunk_data(chunk_info: Tuple[int, int, str, int]) -> pd.DataFrame:
    """Load a specific chunk of data from file"""
    start, end, file_path, app_id = chunk_info
    
    # Read the full file
    df = pd.read_parquet(file_path, columns=PARQUET_COLUMNS)
    
    # Filter and get chunk
    df_filtered = df[(df['review_language'] == DEFAULT_LANGUAGE) & 
                    (df['steam_appid'] == app_id)]
    
    chunk = df_filtered.iloc[start:end].copy()
    
    # Clean up
    del df, df_filtered
    gc.collect()
    
    return chunk


def aggregate_temp_results(temp_paths: Dict[str, str], GAME_THEMES: Dict,
                          game_name_mapping: Dict[int, str]) -> pd.DataFrame:
    """Aggregate all temporary results into final report"""
    # Read all aggregation files
    agg_files = list(Path(temp_paths['aggregations']).glob('*.parquet'))
    
    if not agg_files:
        return pd.DataFrame()
    
    # Combine aggregation data
    all_aggs = []
    for agg_file in agg_files:
        all_aggs.append(pd.read_parquet(agg_file))
    
    agg_df = pd.concat(all_aggs)
    
    # Group by app_id and topic_id to combine counts
    final_agg = agg_df.groupby(['steam_appid', 'topic_id']).agg({
        'review_count': 'sum',
        'likes_sum': 'sum',
        'dislikes_sum': 'sum'
    }).reset_index()
    
    # Collect positive reviews
    positive_reviews_dict = {}
    pos_files = list(Path(temp_paths['positive_reviews']).glob('*.parquet'))
    
    for pos_file in pos_files:
        df = pd.read_parquet(pos_file)
        for _, row in df.iterrows():
            key = (row['steam_appid'], row['topic_id'])
            if key not in positive_reviews_dict:
                positive_reviews_dict[key] = []
            positive_reviews_dict[key].extend(row['reviews'])
    
    # Collect negative reviews
    negative_reviews_dict = {}
    neg_files = list(Path(temp_paths['negative_reviews']).glob('*.parquet'))
    
    for neg_file in neg_files:
        df = pd.read_parquet(neg_file)
        for _, row in df.iterrows():
            key = (row['steam_appid'], row['topic_id'])
            if key not in negative_reviews_dict:
                negative_reviews_dict[key] = []
            negative_reviews_dict[key].extend(row['reviews'])
    
    # Build final report
    final_rows = []
    
    for _, row in final_agg.iterrows():
        app_id = int(row['steam_appid'])
        topic_id = int(row['topic_id'])
        
        # Get theme name
        if app_id in GAME_THEMES:
            theme_keys = list(GAME_THEMES[app_id].keys())
            theme_name = theme_keys[topic_id] if topic_id < len(theme_keys) else f"Unknown Theme {topic_id}"
        else:
            theme_name = f"Unknown Theme {topic_id}"
        
        # Calculate ratios
        total = row['review_count']
        if total > 0:
            like_ratio = f"{(row['likes_sum'] / total * 100):.1f}%"
            dislike_ratio = f"{(row['dislikes_sum'] / total * 100):.1f}%"
        else:
            like_ratio = "0.0%"
            dislike_ratio = "0.0%"
        
        # Get reviews
        key = (app_id, topic_id)
        positive_reviews = positive_reviews_dict.get(key, [])
        negative_reviews = negative_reviews_dict.get(key, [])
        
        final_rows.append({
            'steam_appid': app_id,
            'Theme': theme_name,
            '#Reviews': int(total),
            'Positive': int(row['likes_sum']),
            'Negative': int(row['dislikes_sum']),
            'LikeRatio': like_ratio,
            'DislikeRatio': dislike_ratio,
            'Positive_Reviews': positive_reviews,
            'Negative_Reviews': negative_reviews
        })
    
    return pd.DataFrame(final_rows)