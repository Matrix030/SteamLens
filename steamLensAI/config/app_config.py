#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os

# Create directories if they don't exist
os.makedirs('output_csvs', exist_ok=True)
os.makedirs('checkpoints', exist_ok=True)

# Streamlit Page Configuration
STREAMLIT_PAGE_CONFIG = {
    "page_title": "steamLensAI",
    "layout": "wide",
    "initial_sidebar_state": "expanded"
}

# Default hardware configuration for summarization
HARDWARE_CONFIG = {
    'worker_count': 8, #old - 6
    'memory_per_worker': '4GB', #old - 3GB
    'gpu_batch_size': 256, # old - 96
    'model_name': 'sshleifer/distilbart-cnn-12-6',
    'chunk_size': 800, #old - 400
    'checkpoint_frequency': 15, # old - 25
    'cleanup_frequency': 3, # old - 10
    # Add these new parameters for longer summaries:
    'max_summary_length': 300,  # Increase from default 60, new - 300
    'min_summary_length': 80,   # Increase from default 20, new 80
    'num_beams': 6,            # Increase from old - 2 -new-er -  4 for better quality
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
    'small': 100         # <= 100 app IDs
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
    'small': '256MB' #old 64 MB
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