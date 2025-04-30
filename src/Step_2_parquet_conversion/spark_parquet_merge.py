#!/usr/bin/env python3
"""
Robust Memory-Efficient Parquet File Merger

This script combines multiple Parquet files from a directory structure into a single file
while handling schema variations, corrupt files, and memory constraints.

This version focuses on maximum robustness against malformed files by processing one file at a time.
"""

import os
import sys
import logging
import traceback
from pathlib import Path
from typing import List, Dict, Set, Any, Optional, Tuple
from collections import defaultdict

import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc
import pyarrow.fs

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("merge_parquet_debug.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def discover_parquet_files(base_dir: str) -> List[Path]:
    """Recursively discover all non-empty .parquet files in the directory."""
    base_path = Path(base_dir)
    if not base_path.exists():
        logger.error(f"Base directory {base_dir} does not exist")
        sys.exit(1)
    
    logger.info(f"Scanning for Parquet files in {base_dir}...")
    parquet_files = []
    skipped_files = []
    
    for file_path in base_path.rglob("*.parquet"):
        try:
            # Check if file is non-empty
            file_size = file_path.stat().st_size
            if file_size > 0:
                parquet_files.append(file_path)
                logger.info(f"Found valid Parquet file: {file_path} ({file_size} bytes)")
            else:
                logger.warning(f"Skipping empty file: {file_path}")
                skipped_files.append((str(file_path), "Empty file"))
        except Exception as e:
            logger.error(f"Error accessing file {file_path}: {e}")
            skipped_files.append((str(file_path), f"Access error: {e}"))
    
    logger.info(f"Discovered {len(parquet_files)} non-empty Parquet files")
    logger.info(f"Skipped {len(skipped_files)} files during discovery")
    
    return parquet_files

def test_file_readability(file_path: Path) -> Tuple[bool, Optional[pa.Schema], Optional[str]]:
    """
    Test if a Parquet file can be read and return its schema if possible.
    
    Returns:
        Tuple of (is_readable, schema, error_message)
    """
    try:
        # Try to read the schema first (lightweight)
        schema = pq.read_schema(file_path)
        return True, schema, None
    except Exception as schema_err:
        try:
            # Try to read metadata next
            metadata = pq.read_metadata(file_path)
            # Try alternative method to get schema
            table = pq.read_table(file_path, nrows=5)
            return True, table.schema, None
        except Exception as e:
            error_msg = f"Cannot read file: {e}"
            return False, None, error_msg

def build_unified_schema(valid_files: List[Tuple[Path, pa.Schema]]) -> pa.Schema:
    """
    Build a unified schema that can accommodate all the different file schemas.
    
    Strategy:
    1. Collect all fields from all schemas
    2. For fields that appear in multiple schemas with different types, use string type
    3. Create a new schema with all fields
    """
    all_fields = {}  # Map of field_name -> set of field types
    mixed_type_fields = set()
    
    # First pass: collect all fields and detect type variations
    for file_path, schema in valid_files:
        for field in schema:
            field_name = field.name
            field_type = str(field.type)
            
            if field_name not in all_fields:
                all_fields[field_name] = set()
            
            all_fields[field_name].add(field_type)
            
            # If we now have multiple types, mark as mixed
            if len(all_fields[field_name]) > 1:
                mixed_type_fields.add(field_name)
                logger.warning(f"Column '{field_name}' has mixed types: {all_fields[field_name]}")
                logger.warning(f"  - Latest occurrence in {file_path}")
    
    # Build unified schema
    unified_fields = []
    for field_name, type_set in all_fields.items():
        if field_name in mixed_type_fields:
            # Use string for mixed-type fields
            unified_fields.append(pa.field(field_name, pa.string()))
            logger.info(f"Normalizing column '{field_name}' to string type")
        else:
            # For consistent types, use the first schema's field definition
            for file_path, schema in valid_files:
                if field_name in schema.names:
                    field_idx = schema.names.index(field_name)
                    unified_fields.append(schema.field(field_idx))
                    break
    
    return pa.schema(unified_fields)

def normalize_table_schema(table: pa.Table, unified_schema: pa.Schema, mixed_type_columns: Set[str]) -> pa.Table:
    """
    Normalize a table to match the unified schema.
    
    1. Convert mixed-type columns to string
    2. Add missing columns with null values
    3. Reorder columns to match unified schema
    """
    # First normalize mixed-type columns
    for col_name in mixed_type_columns:
        if col_name in table.column_names:
            idx = table.column_names.index(col_name)
            try:
                string_col = pc.cast(table.column(col_name), pa.string())
                table = table.set_column(idx, col_name, string_col)
            except Exception as e:
                logger.warning(f"Error converting column {col_name} to string: {e}")
                # Create a new column of empty strings as fallback
                string_array = pa.array([''] * len(table), type=pa.string())
                table = table.set_column(idx, col_name, string_array)
    
    # Add missing columns with nulls
    for field in unified_schema:
        if field.name not in table.column_names:
            null_array = pa.nulls(len(table), type=field.type)
            table = table.append_column(field.name, null_array)
    
    # Reorder to match unified schema
    return table.select([field.name for field in unified_schema])

def merge_parquet_files_safely(input_dir: str, output_file: str) -> None:
    """
    Robust implementation that safely handles corrupted files by:
    1. Testing each file individually
    2. Building a unified schema
    3. Writing each file incrementally
    """
    # Step 1: Discover all potential files
    files = discover_parquet_files(input_dir)
    if not files:
        logger.error("No valid Parquet files found")
        return
    
    # Step 2: Test each file's readability and collect schemas
    logger.info("Testing file readability and collecting schemas...")
    valid_files = []
    skipped_files = []
    
    for file_path in files:
        is_readable, schema, error = test_file_readability(file_path)
        if is_readable and schema is not None:
            valid_files.append((file_path, schema))
            logger.info(f"Verified readable file: {file_path}")
        else:
            skipped_files.append((file_path, error or "Unknown error"))
            logger.warning(f"Skipping unreadable file: {file_path}, reason: {error}")
    
    if not valid_files:
        logger.error("No readable Parquet files found")
        return
    
    # Step 3: Build unified schema
    logger.info(f"Building unified schema from {len(valid_files)} valid files...")
    unified_schema = build_unified_schema(valid_files)
    logger.info(f"Unified schema has {len(unified_schema.names)} columns")
    
    # Find mixed-type columns
    mixed_type_columns = set()
    field_types = defaultdict(set)
    
    for _, schema in valid_files:
        for field in schema:
            field_types[field.name].add(str(field.type))
    
    for field_name, types in field_types.items():
        if len(types) > 1:
            mixed_type_columns.add(field_name)
    
    # Step 4: Incrementally process each file
    logger.info(f"Processing {len(valid_files)} files with unified schema...")
    
    # Initialize writer with unified schema
    writer = None
    successful_files = 0
    failed_files = 0
    
    try:
        for file_idx, (file_path, _) in enumerate(valid_files):
            try:
                logger.info(f"Processing file {file_idx+1}/{len(valid_files)}: {file_path}")
                
                # Read the table with error handling
                try:
                    table = pq.read_table(file_path)
                except Exception as e:
                    logger.error(f"Failed to read {file_path} into table: {e}")
                    failed_files += 1
                    continue
                
                # Normalize table to match unified schema
                try:
                    table = normalize_table_schema(table, unified_schema, mixed_type_columns)
                except Exception as e:
                    logger.error(f"Failed to normalize schema for {file_path}: {e}")
                    failed_files += 1
                    continue
                
                # Initialize writer with the first successful table
                if writer is None:
                    try:
                        writer = pq.ParquetWriter(
                            output_file, 
                            unified_schema,
                            compression="snappy",
                            use_dictionary=True,
                            write_statistics=True
                        )
                        logger.info(f"Initialized ParquetWriter with unified schema to {output_file}")
                    except Exception as e:
                        logger.error(f"Failed to create ParquetWriter: {e}")
                        raise
                
                # Write the table
                try:
                    writer.write_table(table)
                    successful_files += 1
                    logger.info(f"Successfully wrote data from {file_path}")
                except Exception as e:
                    logger.error(f"Failed to write table from {file_path}: {e}")
                    failed_files += 1
                    
            except Exception as e:
                logger.error(f"Unexpected error processing {file_path}: {e}")
                logger.error(traceback.format_exc())
                failed_files += 1
        
        # Close the writer
        if writer:
            writer.close()
            logger.info(f"Closed writer, output saved to {output_file}")
        else:
            logger.error("No files were successfully processed, no output created")
            return
        
        # Verify output
        output_path = Path(output_file)
        if output_path.exists():
            file_size = output_path.stat().st_size
            logger.info(f"Output file size: {file_size} bytes")
            
            # Try to verify the output file is readable
            try:
                metadata = pq.read_metadata(output_file)
                logger.info(f"Output file has {metadata.num_rows} rows in {metadata.num_row_groups} row groups")
            except Exception as e:
                logger.warning(f"Output file verification warning: {e}")
        
        # Print summary
        print("\nMERGE SUMMARY:")
        print(f"Total files scanned: {len(files)}")
        print(f"Files successfully processed: {successful_files}")
        print(f"Files skipped due to errors: {failed_files + len(skipped_files)}")
        print(f"Mixed-type columns normalized: {len(mixed_type_columns)}")
        print(f"Output file: {output_file}")
        
        logger.info("Merge operation completed successfully")
        
    except Exception as e:
        logger.error(f"Fatal error during merge operation: {e}")
        logger.error(traceback.format_exc())
        
        # Ensure writer is closed in case of error
        if writer:
            try:
                writer.close()
            except:
                pass
        raise

def main():
    """Main entry point with input validation and error handling."""
    try:
        input_dir = "parquet_output"
        output_file = "all_files.parquet"
        
        merge_parquet_files_safely(input_dir, output_file)
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()