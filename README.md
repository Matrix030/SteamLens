# SteamETL

A comprehensive data pipeline for extracting, transforming, and loading Steam application data into MongoDB for analysis and research.

## Project Overview

SteamETL is a data engineering project that focuses on scraping and storing data from the Steam platform. It collects detailed information about Steam applications (games, software, DLCs) including their metadata and user reviews. The collected data is stored in MongoDB, making it suitable for further analysis and research purposes.

## Key Features

- Retrieves complete list of Steam application IDs via the Steam API
- Collects detailed metadata for each application
- Gathers user reviews with full pagination support 
- Stores data in MongoDB for efficient querying and analysis
- Implements robust resume capability for interrupted collection processes
- Provides extensive logging and error handling
- Includes utilities for data pipeline management

## Project Structure

- **getAppIds.py**: Fetches and sorts all application IDs available on Steam
- **getAppDetails.py**: Collects detailed metadata and user reviews for specific applications
- **steammongo.py**: MongoDB integration module for storing and retrieving Steam data
- **clean_directories.py**: Utility script for maintaining data directory structure
- **kill_pid.py**: Process monitoring and management utility

## How It Works

1. **Data Collection Initialization**:
   - The pipeline begins by collecting all available Steam app IDs using the Steam API
   - App IDs are sorted and stored in a JSON file for systematic processing

2. **Application Data Extraction**:
   - For each app ID, the system fetches detailed metadata from the Steam Store API
   - Data includes app name, description, pricing, requirements, categories, etc.

3. **Review Collection**:
   - The system paginates through all available user reviews for each application
   - Reviews are collected with all associated metadata (helpfulness, playtime, etc.)

4. **Progress Tracking**:
   - The pipeline maintains progress indicators to allow resumption after interruptions
   - Cursor-based pagination ensures no data is missed or duplicated during collection

5. **Data Storage**:
   - All collected data is stored in MongoDB collections for efficient querying
   - Documents are structured to maintain relationships between apps and their reviews

## Installation and Setup

1. Clone the repository
2. Install dependencies:
   ```
   pip install -r requirements.txt
   ```
3. Ensure MongoDB is installed and running
4. Configure MongoDB connection string in the code or environment variables

## Usage

### Collecting App IDs
```python
python New/src/getAppIds.py
```

### Processing Applications
```python
python New/src/getAppDetails.py
```

### Using MongoDB Integration
```python
python New/src/steammongo.py
```

## Technical Details

- **Language**: Python 3.x
- **Database**: MongoDB
- **Key Dependencies**:
  - requests - for API interactions
  - pymongo - for MongoDB operations
  - psutil - for process management

## Data Structure

The collected data follows a hierarchical structure:
- App metadata (name, release date, price, etc.)
- App categories and tags
- App system requirements
- User reviews with detailed metrics

## Performance Considerations

- The system implements rate limiting to respect Steam API constraints
- Progress tracking enables efficient resumption of interrupted processes
- Processing is designed to handle the large volume of Steam's catalog (100,000+ apps)

## Use Cases

- Gaming market research and trend analysis
- Sentiment analysis on game reviews
- Building recommendation systems based on game metadata and user preferences
- Historical analysis of game popularity and reception

## License

This project is intended for research and educational purposes. Always comply with Steam's terms of service when using the collected data. 