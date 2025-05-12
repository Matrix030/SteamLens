#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
summarization.py - Sentiment-based summarization for Steam reviews
Uses transformers to generate summaries of positive and negative reviews
"""

import torch
import pandas as pd
from tqdm.auto import tqdm
from typing import List, Dict, Tuple, Any, Union, Optional
from transformers import pipeline, AutoModelForSeq2SeqLM, AutoTokenizer
from dask.distributed import Future

def prepare_partition(start_idx: int, end_idx: int, final_report: pd.DataFrame) -> pd.DataFrame:
    """Prepare a partition of the final report for processing
    
    Args:
        start_idx (int): Starting index of the partition
        end_idx (int): Ending index of the partition
        final_report (DataFrame): Complete report to partition
        
    Returns:
        DataFrame: Partition of the report
    """
    return final_report.iloc[start_idx:end_idx].copy()

def process_partition(partition_df: pd.DataFrame, worker_id: int, hardware_config: Dict[str, Any], 
                     model_dataset_name: Optional[str] = None, 
                     tokenizer_dataset_name: Optional[str] = None) -> List[Tuple[int, str, str]]:
    """Optimized worker for GPU processing with sentiment separation
    
    Args:
        partition_df (DataFrame): Partition of the final report to process
        worker_id (int): ID of the worker processing this partition
        hardware_config (dict): Configuration for hardware optimization
        model_dataset_name (str, optional): Name of the dataset containing the model
        tokenizer_dataset_name (str, optional): Name of the dataset containing the tokenizer
        
    Returns:
        list: List of tuples (index, positive_summary, negative_summary)
    """
    # Import needed packages
    from transformers import pipeline, AutoModelForSeq2SeqLM, AutoTokenizer
    import torch
    from dask.distributed import get_worker
    
    # Load model components with optimal settings
    print(f"Worker {worker_id} initializing with optimized settings")
    
    # Use published model and tokenizer if dataset names are provided, otherwise load them
    if model_dataset_name is not None and tokenizer_dataset_name is not None:
        # Get the worker and access the datasets
        worker = get_worker()
        model = worker.client.get_dataset(model_dataset_name)
        tokenizer = worker.client.get_dataset(tokenizer_dataset_name)
        print(f"Worker {worker_id} using model and tokenizer from published datasets")
        
        # After receiving the model, move it to the appropriate device
        device = "cuda" if torch.cuda.is_available() else "cpu"
        if device == "cuda":
            # Move to GPU with half precision for speed if GPU is available
            model = model.to(device).half()
            print(f"Worker {worker_id} moved model to {device} with half precision")
        else:
            model = model.to(device)
            print(f"Worker {worker_id} moved model to {device}")
    else:
        # Load tokenizer
        tokenizer = AutoTokenizer.from_pretrained(hardware_config['model_name'])
        
        # Load model with optimized settings
        device = "cuda" if torch.cuda.is_available() else "cpu"
        model = AutoModelForSeq2SeqLM.from_pretrained(
            hardware_config['model_name'],
            torch_dtype=torch.float16 if device == "cuda" else torch.float32,
            device_map="auto",
            low_cpu_mem_usage=True
        )
    
    # Create optimized pipeline
    summarizer = pipeline(
        task='summarization',
        model=model,
        tokenizer=tokenizer,
        framework='pt',
        model_kwargs={
            "use_cache": True,            # Enable caching for speed
            "return_dict_in_generate": True  # More efficient generation
        }
    )
    
    # Report GPU status if available
    if torch.cuda.is_available():
        gpu_mem = torch.cuda.memory_allocated(0) / (1024**3)
        print(f"Worker {worker_id}: GPU Memory: {gpu_mem:.2f}GB allocated")
    
    # Highly optimized batch processing function
    def process_chunks_batched(chunks: List[str]) -> List[str]:
        """Process chunks in large batches for GPU"""
        all_summaries = []
        
        # Use large batches for the GPU
        for i in range(0, len(chunks), hardware_config['gpu_batch_size']):
            batch = chunks[i:i+hardware_config['gpu_batch_size']]
            batch_summaries = summarizer(
                batch,
                max_length=60,
                min_length=20,
                truncation=True,
                do_sample=False,
                num_beams=2  # Use beam search for better quality with minimal speed impact
            )
            all_summaries.extend([s["summary_text"] for s in batch_summaries])
            
            # Minimal cleanup - only when really needed
            if i % (hardware_config['gpu_batch_size'] * 3) == 0 and torch.cuda.is_available():
                torch.cuda.empty_cache()
                    
        return all_summaries
    
    # Optimized hierarchical summary function
    def hierarchical_summary(reviews: List[str]) -> str:
        """Create hierarchical summary with optimized chunk sizes"""
        # Handle edge cases efficiently
        if not reviews or not isinstance(reviews, list) or len(reviews) == 0:
            return "No reviews available for summarization."
        
        # Fast path for small review sets
        if len(reviews) <= hardware_config['chunk_size']:
            doc = "\n\n".join(reviews)
            return summarizer(
                doc,
                max_length=60,
                min_length=20,
                truncation=True,
                do_sample=False
            )[0]['summary_text']
        
        # Process larger review sets with optimized chunking
        all_chunks = []
        for i in range(0, len(reviews), hardware_config['chunk_size']):
            batch = reviews[i:i+hardware_config['chunk_size']]
            text = "\n\n".join(batch)
            all_chunks.append(text)
        
        # Process chunks with optimized batching
        intermediate_summaries = process_chunks_batched(all_chunks)
        
        # Create final summary
        joined = " ".join(intermediate_summaries)
        return summarizer(
            joined,
            max_length=60,
            min_length=20,
            truncation=True,
            do_sample=False
        )[0]['summary_text']
    
    # Process the partition with minimal overhead
    results = []
    
    # Use tqdm for progress tracking
    with tqdm(total=len(partition_df), desc=f"Worker {worker_id}", position=worker_id) as pbar:
        for idx, row in partition_df.iterrows():
            # Process the reviews by sentiment
            positive_reviews = row['Positive_Reviews'] if isinstance(row['Positive_Reviews'], list) else []
            negative_reviews = row['Negative_Reviews'] if isinstance(row['Negative_Reviews'], list) else []
            
            # Generate summaries for both positive and negative reviews
            positive_summary = hierarchical_summary(positive_reviews) if positive_reviews else "No positive reviews available."
            negative_summary = hierarchical_summary(negative_reviews) if negative_reviews else "No negative reviews available."
            
            results.append((idx, positive_summary, negative_summary))
            
            # Minimal cleanup - only every N iterations
            if len(results) % hardware_config['cleanup_frequency'] == 0 and torch.cuda.is_available():
                torch.cuda.empty_cache()
                
            # Update progress bar
            pbar.update(1)
    
    # Final cleanup
    if torch.cuda.is_available():
        torch.cuda.empty_cache()
    
    print(f"Worker {worker_id} completed successfully")
    return results

def update_main_progress(futures: List[Any], progress_bar: Any, stop_flag: List[bool], final_report_length: int) -> None:
    """Thread function to update the main progress bar
    
    Args:
        futures (list): List of Dask futures to monitor
        progress_bar: Streamlit progress bar to update
        stop_flag (list): Mutable flag to signal thread termination
        final_report_length (int): Length of the final report (for progress calculation)
    """
    import time
    start_time = time.time()
    
    while not stop_flag[0]:
        # Count completed futures
        completed_count = sum(f.status == 'finished' for f in futures)
        completed_percentage = completed_count / len(futures) if len(futures) > 0 else 0
        
        # Update progress bar
        progress_bar.progress(completed_percentage)
        
        # Calculate elapsed time
        elapsed_time = time.time() - start_time
        
        # Only check every 2 seconds to reduce overhead
        time.sleep(2) 