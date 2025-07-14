#!/usr/bin/env python3
# -*- coding: utf-8 -*-



import os
import streamlit as st
import pandas as pd
import pyarrow.parquet as pq
from typing import Tuple, Optional, List, Dict, Any

from ..config.app_config import POTENTIAL_NAME_FIELDS, PARQUET_COLUMNS, BLOCKSIZE_THRESHOLDS, BLOCKSIZES, DEFAULT_LANGUAGE

def extract_appid_and_name_from_parquet(file_path: str) -> Tuple[Optional[int], Optional[str]]:
    
    try:
        pf = pq.ParquetFile(file_path)
        
        # Read schema to find available columns among potential_name_fields
        available_schema_fields = pf.schema.names
        columns_to_read = ['steam_appid'] + [col for col in POTENTIAL_NAME_FIELDS if col in available_schema_fields]

        # Read a small sample (e.g., first 1000 rows, or first few row groups) of necessary columns
        # This example reads the first non-empty row group, up to 1000 rows from it.
        # A more robust version might try a few row groups or read a fixed number of rows across groups.
        sample_df = None
        for i in range(min(5, pf.num_row_groups)): # Try up to 5 row groups
            try:
                # Attempt to read a slice of rows from the row group
                # Read up to 100 rows for sampling
                batches = []
                for batch in pf.iter_batches(batch_size=100, columns=columns_to_read):
                    batches.append(batch)
                    if sum(len(b) for b in batches) >= 100: # Limit sample size for initial check
                        break
                if not batches:
                    continue

                sample_df = pd.concat([b.to_pandas() for b in batches])
                if not sample_df.empty:
                    break
            except Exception: # pylint: disable=broad-except
                # If a row group is problematic, try the next one
                continue
        
        if sample_df is None or sample_df.empty:
            st.warning(f"Could not read sample data from {file_path} to extract metadata.")
            return None, None
        
        app_id_series = sample_df['steam_appid'].dropna()
        if app_id_series.empty:
            return None, None
        
        # Get the most common app ID in case there are multiple
        app_id = app_id_series.mode()[0]
        
        game_name = None
        for field in POTENTIAL_NAME_FIELDS:
            if field in sample_df.columns and not sample_df[field].isnull().all():
                non_null_values = sample_df[field].dropna()
                if not non_null_values.empty:
                    game_name = non_null_values.mode()[0]
                    break
                    
        return int(app_id) if pd.notna(app_id) else None, game_name
    except Exception as e:
        st.error(f"Error extracting app ID and name (optimized) from file {os.path.basename(file_path)}: {str(e)}")
        return None, None

def extract_appid_from_parquet(file_path: str) -> Optional[int]:
    
    app_id, _ = extract_appid_and_name_from_parquet(file_path)
    return app_id

def determine_blocksize(file_size_gb: float) -> str:
    
    if file_size_gb > BLOCKSIZE_THRESHOLDS['large']:
        return BLOCKSIZES['large']
    elif file_size_gb > BLOCKSIZE_THRESHOLDS['medium']:
        return BLOCKSIZES['medium']
    else:
        return BLOCKSIZES['small']

def load_theme_dictionary(themes_file: str) -> Dict[int, Dict[str, List[str]]]:
    
    import json
    
    try:
        with open(themes_file, 'r') as f:
            raw = json.load(f)
        return {int(appid): themes for appid, themes in raw.items()}
    except FileNotFoundError:
        st.error(f"Theme file '{themes_file}' not found. Please upload it first.")
        return {}
    except Exception as e:
        st.error(f"Error loading theme dictionary: {str(e)}")
        return {} 