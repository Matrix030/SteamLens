import requests
import json
import time
import urllib.parse
import os
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("../App_Details/steam_app_scraper.log"),
        logging.StreamHandler()
    ]
)

def process_app(appID, num_of_reviews_per_page=1, skip_if_exists=True):
    """Process a single Steam app to fetch its details and reviews."""
    # Set up app-specific variables
    App_Details_url = f"https://store.steampowered.com/api/appdetails?appids={appID}"
    
    app_review_url = (
        f"https://store.steampowered.com/appreviews/{appID}"
        f"?json=1&filter=all&language=all&day_range=9223372036854775807"
        f"&review_type=all&purchase_type=all&num_per_page={num_of_reviews_per_page}"
    )
    
    # Create app-specific directory
    app_dir = f"../App_Details/{appID}_data"
    Review_Detail_json = f"{app_dir}/Review_Details{appID}.json"
    
    # Check if already processed
    if skip_if_exists and os.path.exists(Review_Detail_json):
        logging.info(f"AppID {appID} already processed. Skipping.")
        return "skipped"
    
    # Create directory if it doesn't exist
    os.makedirs(app_dir, exist_ok=True)
    
    def save_progress(current_cursor, next_cursor, num_reviews_till_cursor):
        """Saves the progress of the current appID with the cursor values and review count."""
        current_progress = (
            f"AppID = {appID}\n"
            f"Current Cursor = {current_cursor}\n"
            f"Next Cursor = {next_cursor}\n"
            f"Total_reviews_so_far = {num_reviews_till_cursor}\n"
            f"Last Updated = {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        )
        with open(f"{app_dir}/rt_data", 'w') as file:
            file.write(current_progress)
    
    def get_next_page(next_cursor):
        """Returns the URL for fetching the next page of reviews using the dynamic appID."""
        return app_review_url + f'&cursor={next_cursor}'
    
    def get_total_num_reviews_int(data):
        """Adds the total number of collected reviews to the data structure."""
        reviews_list = data[str(appID)]["data"].get("review_stats", {}).get("reviews", [])
        # Initialize review_stats if missing, and add a count.
        data[str(appID)]["data"].setdefault("review_stats", {})["total_num_reviews"] = len(reviews_list)
        return data
    
    def get_data_json(url):
        """Makes an API call and returns JSON data after a one-second pause."""
        time.sleep(1)  # To respect API rate limits
        try:
            response = requests.get(url)
            if response.status_code == 200:
                logging.info(f"Data retrieved for appID: {appID}")
                return response.json()
            else:
                logging.error(f"Failed to get data for appID: {appID}. Status code: {response.status_code}")
                return None
        except Exception as e:
            logging.error(f"An error occurred for appID {appID}: {e}")
            return None
    
    def get_reviews(review_data, app_details_data):
        """Iteratively fetches review pages and appends reviews to the app details."""
        current_cursor = ""
        # URL encode the cursor value
        next_cursor = urllib.parse.quote(review_data.get("cursor", ""))
        num_reviews_till_cursor = 1
    
        # Ensure "review_stats" exists in the app details data
        app_details_data.setdefault(str(appID), {}).setdefault("data", {}).setdefault("review_stats", {"reviews": []})
    
        # Use 'and' to continue only if both conditions for a new page are met
        while (review_data["query_summary"]["num_reviews"] != 0) and (next_cursor != current_cursor):
            for item in review_data["reviews"]:
                app_details_data[str(appID)]["data"]["review_stats"]["reviews"].append(item)
            
            logging.info(f"Going to next page for appID: {appID}")
            current_cursor = next_cursor
            logging.info(f"num_reviews on this page = {review_data['query_summary']['num_reviews']}")
            
            # Encode the current cursor before using it in the next URL
            encoded_cursor = urllib.parse.quote(review_data["cursor"])
            next_page_url = get_next_page(encoded_cursor)
            review_data = get_data_json(next_page_url)
            
            if review_data is None:
                logging.warning(f"No data received for next page for appID: {appID}. Exiting loop.")
                break
    
            if "cursor" in review_data:
                # Update cursors and review count using the actual number of reviews returned
                current_cursor = encoded_cursor
                next_cursor = urllib.parse.quote(review_data["cursor"])
                num_reviews_till_cursor += review_data["query_summary"]["num_reviews"]
            
            else:
                logging.warning(f"Cursor not found in the response for appID: {appID}. Exiting loop.")
                break
            save_progress(current_cursor, next_cursor, num_reviews_till_cursor)
            
            # Save progress to a JSON file after each page iteration
            with open(Review_Detail_json, 'w') as file:
                json.dump(app_details_data, file, indent=1)
        
        logging.info(f"Either JSON was empty or all reviews have been extracted for appID: {appID}.")
        return app_details_data
    
    # Main processing for this app
    try:
        logging.info(f"Processing appID: {appID}")
        app_details = get_data_json(App_Details_url)
        
        if app_details is None:
            logging.error(f"Failed to fetch app details for appID: {appID}")
            return "failure"
        
        if not app_details.get(str(appID), {}).get("success", False):
            logging.error(f"App details request was not successful for appID: {appID}")
            return "failure"
        
        reviews = get_data_json(app_review_url)
        if reviews is None:
            logging.error(f"Failed to fetch reviews for appID: {appID}")
            return "failure"
        
        app_details_with_review = get_reviews(reviews, app_details)
        app_details_final = get_total_num_reviews_int(app_details_with_review)
        
        with open(Review_Detail_json, 'w') as file:
            json.dump(app_details_final, file, indent=1)
        
        logging.info(f"Successfully processed appID: {appID}")
        return "success"
    except Exception as e:
        logging.error(f"Error processing appID {appID}: {e}")
        return "failure"

def load_app_ids(file_path):
    """Load the list of app IDs from the JSON file."""
    try:
        with open(file_path, 'r') as file:
            data = json.load(file)
            return [app["appid"] for app in data["applist"]["apps"]]
    except Exception as e:
        logging.error(f"Error loading app IDs from {file_path}: {e}")
        return []

if __name__ == "__main__":
    # Load app IDs from the JSON file
    app_ids_file = "../appIdData/sorted_steam_apps.json"
    app_ids = load_app_ids(app_ids_file)
    
    if not app_ids:
        logging.error("No app IDs found or error loading app IDs. Exiting.")
        exit()
    
    logging.info(f"Loaded {len(app_ids)} app IDs.")
    
    # Create the main App_Details directory if it doesn't exist
    os.makedirs("../App_Details", exist_ok=True)
    
    # Check if there's a resumption point
    resume_from = 0
    resume_file = "../App_Details/resume_point.txt"
    if os.path.exists(resume_file):
        with open(resume_file, 'r') as file:
            try:
                resume_from = int(file.read().strip())
                logging.info(f"Resuming from app ID index: {resume_from}")
            except:
                logging.warning("Invalid resume point. Starting from the beginning.")
    
    # Process each app ID
    success_count = 0
    failure_count = 0
    skip_count = 0
    
    try:
        for i, app_id in enumerate(app_ids[resume_from:], start=resume_from):
            # Add a delay between apps to avoid rate limiting
            if i > resume_from:
                time.sleep(2)  # 2-second delay between apps
            
            # Process the app
            result = process_app(app_id, num_of_reviews_per_page=100, skip_if_exists=True)
            if result == "success":
                success_count += 1
            elif result == "skipped":
                skip_count += 1
            else:  # "failure"
                failure_count += 1
            
            # Save resume point
            with open(resume_file, 'w') as file:
                file.write(str(i + 1))
            
            # Print progress
            total_processed = i + 1
            logging.info(f"Progress: {total_processed}/{len(app_ids)} apps processed. "
                      f"Success: {success_count}, Failures: {failure_count}, Skipped: {skip_count}")
    except KeyboardInterrupt:
        logging.warning("Process interrupted by user. Progress has been saved.")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
    finally:
        logging.info(f"Process completed or interrupted. "
                   f"Success: {success_count}, Failures: {failure_count}, Skipped: {skip_count}")