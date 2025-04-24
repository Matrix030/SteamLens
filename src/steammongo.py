"""
Steam App Data Scraper
----------------------
This script fetches app details and user reviews from the Steam API and stores
them in MongoDB while maintaining progress tracking in the filesystem.
"""

import requests
import json
import time
import urllib.parse
import os
import logging
from datetime import datetime
from pymongo import MongoClient

# =============================================================================
# SETUP AND CONFIGURATION
# =============================================================================

def setup_logging():
    """Configure the application logging."""
    log_dir = "../App_Details"
    os.makedirs(log_dir, exist_ok=True)
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(f"{log_dir}/steam_app_scraper.log"),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

def get_mongodb_connection():
    """Create and return a MongoDB connection."""
    mongo_uri = os.environ.get("MONGO_URI", "mongodb://localhost:27017/")
    client = MongoClient(mongo_uri)
    db = client["steam_data"]
    return db

# =============================================================================
# API INTERACTION
# =============================================================================

def fetch_data_from_api(url, app_id):
    """
    Make an API request and return the JSON response.
    
    Args:
        url: The API endpoint URL
        app_id: The Steam app ID for logging purposes
    
    Returns:
        The JSON response or None if the request failed
    """
    time.sleep(0.1)  # To respect API rate limits
    try:
        response = requests.get(url)
        if response.status_code == 200:
            logger.info(f"Data retrieved for appID: {app_id}")
            return response.json()
        else:
            logger.error(f"Failed to get data for appID: {app_id}. Status code: {response.status_code}")
            return None
    except Exception as e:
        logger.error(f"An error occurred for appID {app_id}: {e}")
        return None

# =============================================================================
# PROGRESS TRACKING
# =============================================================================

def save_progress_to_file(app_id, current_cursor, next_cursor, reviews_count, rt_data_file):
    """
    Save the current scraping progress to a text file.
    
    Args:
        app_id: The Steam app ID
        current_cursor: The current pagination cursor
        next_cursor: The next pagination cursor
        reviews_count: The total number of reviews fetched so far
        rt_data_file: The file path for the progress data
    """
    current_progress = (
        f"AppID = {app_id}\n"
        f"Current Cursor = {current_cursor}\n"
        f"Next Cursor = {next_cursor}\n"
        f"Total_reviews_so_far = {reviews_count}\n"
        f"Last Updated = {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
    )
    with open(rt_data_file, 'w') as file:
        file.write(current_progress)

def read_progress_from_file(rt_data_file):
    """
    Read the scraping progress from a text file.
    
    Args:
        rt_data_file: The file path for the progress data
    
    Returns:
        A tuple of (cursor, review_count) or (None, 0) if file cannot be read
    """
    try:
        with open(rt_data_file, 'r') as file:
            rt_data_content = file.read()
            
            cursor = None
            review_count = 0
            
            for line in rt_data_content.split('\n'):
                if line.startswith("Next Cursor = "):
                    cursor = line.replace("Next Cursor = ", "").strip()
                elif line.startswith("Total_reviews_so_far = "):
                    review_count = int(line.replace("Total_reviews_so_far = ", "").strip())
            
            return cursor, review_count
    except Exception as e:
        logger.error(f"Failed to parse resume data: {e}")
        return None, 0

# =============================================================================
# REVIEW PROCESSING
# =============================================================================

def process_app_reviews(app_id, app_details, review_data, start_cursor="", start_count=0, rt_data_file=None):
    """
    Process and collect all reviews for an app by paginating through the Steam API.
    
    Args:
        app_id: The Steam app ID
        app_details: The app details data structure
        review_data: The initial review data from the first API call
        start_cursor: The pagination cursor to start from (for resuming)
        start_count: The number of reviews already collected (for resuming)
        rt_data_file: The file path to save progress
    
    Returns:
        The updated app details data with all reviews appended
    """
    # Set up app-specific review URL
    app_review_url = (
        f"https://store.steampowered.com/appreviews/{app_id}"
        f"?json=1&filter=all&language=all&day_range=9223372036854775807"
        f"&review_type=all&purchase_type=all&num_per_page=100"
    )
    
    # Connect to MongoDB
    db = get_mongodb_connection()
    app_collection = db["app_details"]
    
    current_cursor = ""
    # URL encode the cursor value if we're starting fresh
    if not start_cursor:
        next_cursor = urllib.parse.quote(review_data.get("cursor", ""))
    else:
        # Use the provided cursor if resuming
        next_cursor = start_cursor
    
    review_count = start_count
    
    # Make sure the review_stats structure exists in app_details
    app_details.setdefault(str(app_id), {}).setdefault("data", {}).setdefault("review_stats", {"reviews": []})
    
    # If resuming, we need to fetch the first page using the saved cursor
    if start_cursor:
        first_page_url = app_review_url + f'&cursor={start_cursor}'
        review_data = fetch_data_from_api(first_page_url, app_id)
        if review_data is None:
            logger.error(f"Failed to fetch reviews from saved cursor for appID {app_id}")
            return app_details
    
    # Paginate through all review pages
    while (review_data["query_summary"]["num_reviews"] != 0) and (next_cursor != current_cursor):
        # Append reviews from this page
        for item in review_data["reviews"]:
            app_details[str(app_id)]["data"]["review_stats"]["reviews"].append(item)
        
        logger.info(f"Going to next page for appID: {app_id}")
        current_cursor = next_cursor
        logger.info(f"num_reviews on this page = {review_data['query_summary']['num_reviews']}")
        
        # Encode the current cursor before using it in the next URL
        encoded_cursor = urllib.parse.quote(review_data["cursor"])
        next_page_url = app_review_url + f'&cursor={encoded_cursor}'
        review_data = fetch_data_from_api(next_page_url, app_id)
        
        if review_data is None:
            logger.warning(f"No data received for next page for appID: {app_id}. Exiting loop.")
            break

        if "cursor" in review_data:
            # Update cursors and review count
            current_cursor = encoded_cursor
            next_cursor = urllib.parse.quote(review_data["cursor"])
            review_count += review_data["query_summary"]["num_reviews"]
        else:
            logger.warning(f"Cursor not found in the response for appID: {app_id}. Exiting loop.")
            break
        
        # Save progress to file and database
        if rt_data_file:
            save_progress_to_file(app_id, current_cursor, next_cursor, review_count, rt_data_file)
        
        # Save current state to MongoDB
        app_collection.update_one(
            {"appid": str(app_id)},
            {"$set": app_details},
            upsert=True
        )
    
    logger.info(f"Reviews collection completed for appID: {app_id}. Total reviews: {review_count}")
    
    # Calculate and add the total review count to the data
    reviews_list = app_details[str(app_id)]["data"].get("review_stats", {}).get("reviews", [])
    app_details[str(app_id)]["data"].setdefault("review_stats", {})["total_num_reviews"] = len(reviews_list)
    
    return app_details

# =============================================================================
# MAIN APP PROCESSING
# =============================================================================

def process_app(app_id, skip_if_exists=True):
    """
    Process a single Steam app to fetch its details and reviews.
    
    Args:
        app_id: The Steam app ID
        skip_if_exists: Whether to skip processing if app data already exists
    
    Returns:
        "success", "skipped", or "failure" status
    """
    # Connect to MongoDB
    db = get_mongodb_connection()
    app_collection = db["app_details"]
    
    # Set up app-specific variables
    app_details_url = f"https://store.steampowered.com/api/appdetails?appids={app_id}"
    app_review_url = (
        f"https://store.steampowered.com/appreviews/{app_id}"
        f"?json=1&filter=all&language=all&day_range=9223372036854775807"
        f"&review_type=all&purchase_type=all&num_per_page=100"
    )
    
    # Create app-specific directory for rt_data
    app_dir = f"../App_Details/{app_id}_data"
    rt_data_file = f"{app_dir}/rt_data"
    os.makedirs(app_dir, exist_ok=True)
    
    # Check for existing app in MongoDB
    existing_app = app_collection.find_one({"appid": str(app_id)})
    
    # Check if we need to resume collection
    resume_review_collection = False
    initial_cursor = None
    initial_reviews_count = 0
    
    if os.path.exists(rt_data_file) and existing_app:
        initial_cursor, initial_reviews_count = read_progress_from_file(rt_data_file)
        if initial_cursor:
            resume_review_collection = True
            logger.info(f"Resuming review collection for appID {app_id} from cursor: {initial_cursor}")
            logger.info(f"Already collected {initial_reviews_count} reviews for this app")
    
    # Check if already completely processed (exists in MongoDB and no rt_data file)
    if skip_if_exists and existing_app and not os.path.exists(rt_data_file):
        logger.info(f"AppID {app_id} already fully processed. Skipping.")
        return "skipped"
    
    # Main processing
    try:
        logger.info(f"Processing appID: {app_id}")
        
        # Process based on whether we're starting fresh or resuming
        if not resume_review_collection or not existing_app:
            # Starting fresh: fetch app details and reviews
            app_details = fetch_data_from_api(app_details_url, app_id)
            
            if app_details is None:
                logger.error(f"Failed to fetch app details for appID: {app_id}")
                return "failure"
            
            if not app_details.get(str(app_id), {}).get("success", False):
                logger.error(f"App details request was not successful for appID: {app_id}")
                return "failure"
            
            # Fetch first page of reviews
            reviews = fetch_data_from_api(app_review_url, app_id)
            if reviews is None:
                logger.error(f"Failed to fetch reviews for appID: {app_id}")
                return "failure"
            
            # Process all reviews
            app_details_with_reviews = process_app_reviews(
                app_id, 
                app_details, 
                reviews,
                rt_data_file=rt_data_file
            )
        else:
            # Resuming: continue from where we left off
            # Process remaining reviews starting from saved cursor
            app_details_with_reviews = process_app_reviews(
                app_id,
                existing_app,
                {"query_summary": {"num_reviews": 1}},  # Dummy data, will be replaced
                start_cursor=initial_cursor,
                start_count=initial_reviews_count,
                rt_data_file=rt_data_file
            )
        
        # Save the final data to MongoDB
        app_collection.update_one(
            {"appid": str(app_id)},
            {"$set": app_details_with_reviews},
            upsert=True
        )
        
        # Clean up the rt_data file since processing is complete
        if os.path.exists(rt_data_file):
            os.remove(rt_data_file)
        
        logger.info(f"Successfully processed appID: {app_id}")
        return "success"
    except Exception as e:
        logger.error(f"Error processing appID {app_id}: {e}")
        return "failure"

# =============================================================================
# APP ID LIST PROCESSING
# =============================================================================

def load_app_ids(file_path):
    """
    Load the list of app IDs from the JSON file.
    
    Args:
        file_path: Path to the JSON file with app IDs
        
    Returns:
        List of app IDs or empty list if file cannot be read
    """
    try:
        with open(file_path, 'r') as file:
            data = json.load(file)
            return [app["appid"] for app in data["applist"]["apps"]]
    except Exception as e:
        logger.error(f"Error loading app IDs from {file_path}: {e}")
        return []

def process_all_apps(app_ids, resume_from=0):
    """
    Process a list of Steam app IDs, with progress tracking.
    
    Args:
        app_ids: List of app IDs to process
        resume_from: Index to start processing from
        
    Returns:
        Tuple of (success_count, failure_count, skip_count)
    """
    # Set up counters
    success_count = 0
    failure_count = 0
    skip_count = 0
    resume_file = "../App_Details/resume_point.txt"
    
    try:
        for i, app_id in enumerate(app_ids[resume_from:], start=resume_from):
            # Add a delay between apps to avoid rate limiting
            if i > resume_from:
                time.sleep(0.5)
            
            # Process the app
            result = process_app(app_id, skip_if_exists=True)
            if result == "success":
                success_count += 1
            elif result == "skipped":
                skip_count += 1
            else:  # "failure"
                failure_count += 1
            
            # Save resume point to file
            with open(resume_file, 'w') as file:
                file.write(str(i + 1))
            
            # Print progress
            total_processed = i + 1
            logger.info(f"Progress: {total_processed}/{len(app_ids)} apps processed. "
                      f"Success: {success_count}, Failures: {failure_count}, Skipped: {skip_count}")
    except KeyboardInterrupt:
        logger.warning("Process interrupted by user. Progress has been saved.")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    
    return success_count, failure_count, skip_count

# =============================================================================
# MAIN SCRIPT
# =============================================================================

if __name__ == "__main__":
    # Set up logging
    logger = setup_logging()
    
    # Create main directory
    os.makedirs("../App_Details", exist_ok=True)
    
    # Load app IDs
    app_ids_file = "../appIdData/sorted_steam_apps.json"
    app_ids = load_app_ids(app_ids_file)
    
    if not app_ids:
        logger.error("No app IDs found or error loading app IDs. Exiting.")
        exit()
    
    logger.info(f"Loaded {len(app_ids)} app IDs.")
    
    # Check for resume point
    resume_from = 0
    resume_file = "../App_Details/resume_point.txt"
    if os.path.exists(resume_file):
        with open(resume_file, 'r') as file:
            try:
                resume_from = int(file.read().strip())
                logger.info(f"Resuming from app ID index: {resume_from}")
            except:
                logger.warning("Invalid resume point. Starting from the beginning.")
    
    # Process all apps
    success, failures, skipped = process_all_apps(app_ids, resume_from)
    
    # Log final results
    logger.info(f"Process completed. Success: {success}, Failures: {failures}, Skipped: {skipped}")