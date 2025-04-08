import requests
import json
import time
import urllib.parse  # Import urllib.parse for URL encoding

App_Details_json = "../App_Details/App_Details1091500.json"
Review_Detail_json = "../App_Details/Review_Details1091500.json"
url = "https://store.steampowered.com/api/appdetails?appids=1091500"
app_review_url = "https://store.steampowered.com/appreviews/1091500?json=1&filter=all&language=all&day_range=9223372036854775807&review_type=all&purchase_type=all&num_per_page=100"

def check_success(response):
    return response["success"]
    
def get_data(url):
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
    current_cursor = ""
    # URL encode the cursor value right away
    next_cursor = urllib.parse.quote(review_data["cursor"])
    while review_data["query_summary"]["num_reviews"] != 0 or next_cursor != current_cursor:
        for items in review_data["reviews"]:
            if "review_stats" not in app_details_data["1091500"]["data"]:
                app_details_data["1091500"]["data"]["review_stats"] = {"reviews": []}
            app_details_data["1091500"]["data"]["review_stats"]["reviews"].append(items)
            print(f'appending {items["recommendationid"]}')

        for items in app_details_data["1091500"]["data"]["review_stats"]["reviews"]:
            print(f"{items['recommendationid']} - is present in app_details_data")
        print("Going to next Page")
        current_cursor = next_cursor
        print(f"num_reviews = {review_data["query_summary"]["num_reviews"]}")
        time.sleep(2)  # Add a delay of 2 seconds between API calls
        # Encode the new cursor before appending it to the URL
        encoded_cursor = urllib.parse.quote(review_data["cursor"])
        review_data = get_data(f"https://store.steampowered.com/appreviews/1091500?json=1&filter=all&language=all&day_range=9223372036854775807&review_type=all&purchase_type=all&num_per_page=100&cursor={encoded_cursor}")
        # Ensure that we update the next_cursor safely
        if "cursor" in review_data:
            current_cursor = encoded_cursor
            next_cursor = urllib.parse.quote(review_data["cursor"])
        else:
            print("Cursor not found in the response. Exiting loop.")
            break
    
    print("Either JSON was empty or all reviews have been extracted.")
    return app_details_data
if __name__ == "__main__":
    app_details = get_data(url)
    time.sleep(2)  # Add a delay of 2 seconds between API calls
    print(app_details["1091500"]["success"])
    with open(App_Details_json, 'w') as file:
        json.dump(app_details, file, indent=1)
    reviews = get_data(app_review_url)
    app_detail_review = get_reviews(reviews, app_details)
    with open(Review_Detail_json, 'w') as file:
        json.dump(app_detail_review, file, indent=1)
