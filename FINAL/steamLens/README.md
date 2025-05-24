# 🎮 SteamLens

SteamLens is a powerful sentiment analysis and summarization tool for Steam game reviews. It processes large volumes of game reviews using distributed computing and advanced NLP techniques to provide meaningful insights into player feedback.

## 🧠 Core Processing Logic

### 1. File Processing (`process_files.py`)
- Handles large-scale review data processing using Dask for distributed computing
- Key features:
  - Dynamic resource allocation based on system capabilities
  - Efficient parquet file reading with optimized block sizes
  - Multi-worker processing with GPU acceleration when available
  - Real-time progress monitoring via Dask dashboard
  - Memory-efficient processing of large datasets

### 2. Topic Assignment (`topic_assignment.py`)
- Assigns reviews to specific game themes using semantic similarity
- Features:
  - Uses Sentence Transformers for semantic embedding
  - GPU-accelerated embedding computation
  - Efficient cosine similarity matching
  - Batch processing for improved performance
  - Game-specific theme matching

### 3. Review Summarization (`summarization.py`)
- Generates concise summaries of positive and negative reviews
- Capabilities:
  - Hierarchical summarization for large review sets
  - GPU-optimized batch processing
  - Memory-efficient processing with automatic cleanup
  - Parallel processing across multiple workers
  - Configurable summary length and quality parameters

### 4. Summarization Orchestration (`summarize_processor.py`)
- Manages and coordinates the summarization process
- Features:
  - Hardware-aware resource allocation
  - Dynamic worker scaling based on GPU availability
  - Real-time progress monitoring and dashboard integration
  - Robust error handling and fallback mechanisms
  - Efficient partition management for parallel processing
  - Automatic result saving and timing metrics

## 📋 Requirements

- Python >= 3.6
- Dependencies:
  - streamlit
  - pandas
  - numpy
  - dask[distributed]
  - pyarrow
  - sentence-transformers
  - accelerate
  - transformers
  - torch
  - scikit-learn

## 🚀 Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/steamLens.git
cd steamLens
```

2. Install the package:
```bash
pip install -e .
```

## 💻 Usage

Run the application using:
```bash
steamLens
```

Or directly with Python:
```bash
python -m steamLens
```

## 🔧 Processing Configuration

### Resource Allocation
- Automatically detects system resources (CPU cores, memory)
- Dynamically allocates workers based on available resources
- Configurable memory limits per worker
- GPU acceleration when available
- Adaptive worker scaling (up to 6 workers for GPU, 4 for CPU)

### Performance Optimization
- Configurable batch sizes for GPU processing
- Memory-efficient data processing
- Automatic cleanup of GPU memory
- Parallel processing across multiple workers
- Balanced partition management for optimal parallelization

### Data Processing Parameters
- Configurable block sizes for parquet files
- Adjustable chunk sizes for summarization
- Customizable summary length and quality
- Configurable cleanup frequency
- Automatic partition size optimization

## 📁 Project Structure

```
steamLens/
├── processing/
│   ├── process_files.py     # Main file processing logic
│   ├── topic_assignment.py  # Theme assignment logic
│   ├── summarization.py     # Review summarization logic
│   └── summarize_processor.py # Summarization orchestration
├── app.py                   # Main application entry point
├── __main__.py             # Package entry point
└── setup.py                # Package configuration
```

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## 📝 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🙏 Acknowledgments

- Built with Streamlit
- Uses sentence-transformers for semantic analysis
- Powered by Dask for distributed computing
- GPU acceleration with PyTorch 