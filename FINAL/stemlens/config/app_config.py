#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
app_config.py - Configuration parameters for SteamLens
Contains constants, defaults, and configuration settings
"""

import os

# Create directories if they don't exist
os.makedirs('output_csvs', exist_ok=True)
os.makedirs('checkpoints', exist_ok=True)

# Streamlit Page Configuration
STREAMLIT_PAGE_CONFIG = {
    "page_title": "SteamLens - Sentiment Analysis",
    "page_icon": "🎮",
    "layout": "wide",
    "initial_sidebar_state": "expanded"
}

# Default hardware configuration for summarization
HARDWARE_CONFIG = {
    'worker_count': 6,                # Default for high-end CPU
    'memory_per_worker': '3GB',       # Adjust based on available RAM
    'gpu_batch_size': 96,             # Adjust based on GPU VRAM
    'model_name': 'sshleifer/distilbart-cnn-12-6',  # Efficient summarization model
    'chunk_size': 400,                # Chunk size for processing reviews
    'checkpoint_frequency': 25,       # Frequency of checkpoints
    'cleanup_frequency': 10,          # Frequency of memory cleanup
}

# Default file and path settings
DEFAULT_THEME_FILE = "game_themes.json"
DEFAULT_OUTPUT_PATH = "output_csvs/sentiment_summaries.csv"
DEFAULT_INTERIM_PATH = "output_csvs/sentiment_report.csv"

# Model settings
SENTENCE_TRANSFORMER_MODEL = 'all-MiniLM-L6-v2'

# Default language for filtering reviews
DEFAULT_LANGUAGE = 'english'

# Default batch sizes for different numbers of app IDs
APP_ID_BATCH_SIZES = {
    'very_large': 3,    # > 1000 app IDs
    'large': 5,         # > 500 app IDs
    'medium': 10,       # > 100 app IDs
    'small': 20         # <= 100 app IDs
}

# File size thresholds for blocksize determination
BLOCKSIZE_THRESHOLDS = {
    'large': 1.0,       # > 1GB
    'medium': 0.1,      # > 100MB
    'small': 0.0        # <= 100MB
}

# Blocksizes for different file sizes
BLOCKSIZES = {
    'large': '16MB',
    'medium': '32MB',
    'small': '64MB'
}

# Fields that might contain game name in Parquet files
POTENTIAL_NAME_FIELDS = [
    'name', 
    'game_name', 
    'title', 
    'short_description', 
    'about_the_game'
]

# Columns to read from Parquet files
PARQUET_COLUMNS = [
    'steam_appid', 
    'review', 
    'review_language', 
    'voted_up'
] 