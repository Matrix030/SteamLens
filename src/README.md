# Steam ETL Pipeline

A Python-based ETL (Extract, Transform, Load) pipeline for processing Steam game reviews data.

## Overview

This project provides tools for transforming Steam game review data from JSON format to Parquet format, optimizing for storage and analytical processing. It includes utilities for handling large datasets with robust error handling and memory efficiency.

## Key Components

- **json_to_parquet.py**: Converts JSON review data to Parquet format
- **parquet_merge.py**: Robust utility to merge multiple Parquet files into a single file
- **ETL_Spark.ipynb**: Jupyter notebook for analyzing the Parquet data using PySpark
- **review_numbers.py**: Utility script for counting review statistics

## Features

- Memory-efficient processing of large datasets
- Schema unification across different data files
- Robust error handling for corrupted or malformed files
- Parallel processing capabilities using PySpark
- Support for incremental data processing

## Requirements

- Python 3.6+
- pandas
- pyarrow
- pyspark (for Spark-based processing)
- Jupyter (for notebook execution)

## Usage

### Converting JSON to Parquet

```bash
python json_to_parquet.py
```

This script scans the `../App_Details/` directory for Steam app data and converts review JSON files to Parquet format.

### Merging Parquet Files

```bash
python parquet_merge.py
```

This utility combines multiple Parquet files into a single file while handling schema variations and corrupted files.

### Spark Analytics

Open `ETL_Spark.ipynb` in Jupyter to run PySpark analytics on the processed data.

## Data Structure

The pipeline processes Steam review data with the following key fields:

- Basic game information (appid, title, price, etc.)
- Game metadata (genres, categories, achievements)
- Review content and metrics (text, language, votes)

## Output

The processed data is stored in:

- Individual Parquet files in `parquet_output/` directory
- Merged data in `all_files.parquet`

## License

[Your License Information] 