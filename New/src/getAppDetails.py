import requests
import json
import time
import urllib.parse

# Global variables
appID = 2263220
num_of_reviews_per_page = 1

App_Details_url = f"https://store.steampowered.com/api/appdetails?appids={appID}"

app_review_url = (
    f"https://store.steampowered.com/appreviews/{appID}"
    f"?json=1&filter=all&language=all&day_range=9223372036854775807"
    f"&review_type=all&purchase_type=all&num_per_page={num_of_reviews_per_page}"
)

Review_Detail_json = f"../App_Details/Review_Details{appID}.json"




def save_progress(current_cursor, next_cursor, num_reviews_till_cursor):
    """Saves the progress of the current appID with the cursor values and review count."""
    current_progress = (
        f"AppID = {appID}\n"
        f"Current Cursor = {current_cursor}\n"
        f"Next Cursor = {next_cursor}\n"
        f"Total_reviews_so_far = {num_reviews_till_cursor}\n"
    )
    with open("../App_Details/rt_data", 'w') as file:
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
            print("data retrieved")
            return response.json()
        else:
            print("failed to get data")
            return None
    except Exception as e:
        print(f"An error occurred: {e}")
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
        
        print("Going to next page")
        current_cursor = next_cursor
        print(f"num_reviews on this page = {review_data['query_summary']['num_reviews']}")
        
        # Encode the current cursor before using it in the next URL
        encoded_cursor = urllib.parse.quote(review_data["cursor"])
        next_page_url = get_next_page(encoded_cursor)
        review_data = get_data_json(next_page_url)
        
        if review_data is None:
            print("No data received for next page. Exiting loop.")
            break

        if "cursor" in review_data:
            # Update cursors and review count using the actual number of reviews returned
            current_cursor = encoded_cursor
            next_cursor = urllib.parse.quote(review_data["cursor"])
            num_reviews_till_cursor += review_data["query_summary"]["num_reviews"]
        
        else:
            print("Cursor not found in the response. Exiting loop.")
            break
        save_progress(current_cursor, next_cursor, num_reviews_till_cursor)
        
        # Save progress to a JSON file after each page iteration
        with open(Review_Detail_json, 'w') as file:
            json.dump(app_details_data, file, indent=1)
    
    print("Either JSON was empty or all reviews have been extracted.")
    return app_details_data

if __name__ == "__main__":
    app_details = get_data_json(App_Details_url)
    if app_details[f'{appID}']["success"] is False:
        print("Failed to fetch app details.")
        exit()
    
    reviews = get_data_json(app_review_url)
    if reviews is None:
        print("Failed to fetch reviews.")
        exit()
    
    app_details_with_review = get_reviews(reviews, app_details)
    app_details_final = get_total_num_reviews_int(app_details_with_review)
    
    with open(Review_Detail_json, 'w') as file:
        json.dump(app_details_final, file, indent=1)
