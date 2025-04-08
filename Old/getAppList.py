import requests
import json
import csv

url = "https://api.steampowered.com/ISteamApps/GetAppList/v0002/?key=STEAMKEY&format=json"
json_path = 'AppId_data/appId.json'
csv_path = 'AppId_data/appId.csv'

def get_request(url):
    return requests.get(url)

def get_data(response):
    if response.status_code == 200:
        print("data retrieved")
        return response.json()
    else:
        print("failed to get data")
        return None

def sort_by_appId(data):
    if not data:
        print("No data to sort")
        return None

    appId = data["applist"]["apps"]

    # Remove duplicates by creating a dictionary with appid as key
    # This will automatically keep only one entry per appid
    unique_apps = {}
    for app in appId:
        app_id = app["appid"]
        if app_id not in unique_apps:
            unique_apps[app_id] = app

    # Convert back to list and sort
    unique_app_list = list(unique_apps.values())
    sorted_apps = sorted(unique_app_list, key=lambda app: app["appid"])

    # Update the original data structure
    data["applist"]["apps"] = sorted_apps

    # Save only the sorted apps list to file
    with open(json_path, 'w') as file:
        json.dump(sorted_apps, file, indent=1)

    print(f"Sorted {len(sorted_apps)} unique apps by appid in ascending order.")

    return data

def json_to_csv(json_path, csv_path):
    with open(json_path, 'r', encoding='utf-8') as file:
        data = json.load(file)

    with open(csv_path, 'w', newline='', encoding='utf-8') as csv_file:
        csv_writer = csv.writer(csv_file)

        # Get headers from first item
        if data:
            headers = data[0].keys()
            csv_writer.writerow(headers)

            # Write each row
            for item in data:
                # Handle potential newlines in values by replacing them
                values = [str(v).replace('\n', ' ').replace('\r', ' ') for v in item.values()]
                csv_writer.writerow(values)

    print('CSV conversion finished')

if __name__ == "__main__":
    response = get_request(url)
    data = get_data(response)
    sort_by_appId(data)
    json_to_csv(json_path, csv_path)