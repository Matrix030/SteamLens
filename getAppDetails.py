import requests
import json
import csv

App_Details_json = "App_Details/App_Details1091500.json"
App_Details_csv = "App_Details/App_Details.csv"
url = "https://store.steampowered.com/api/appdetails?appids=1091500"

def get_request(url):
    return requests.get(url)

def get_data(response):
    if response.status_code == 200:
        print("data retrieved")
        return response.json()
    
    
    else:
        print("failed to get data")
        return None
    
def json_to_csv(json_path, csv_path):
    with open(json_path, 'r', encoding='utf-8') as file:
        data = json.load(file)

    with open(csv_path, 'w', newline='', encoding='utf-8') as csv_file:
        csv_writer = csv.writer(csv_file)

        # Get headers from first item
        if data:
            headers = data["1091500"]["data"].keys()
            csv_writer.writerow(headers)

            # Write each row
            for item in data:
                # Handle potential newlines in values by replacing them
                # values = [str(v).replace('\n', ' ').replace('\r', ' ') for v in item.values()]
                csv_writer.writerow(item)

    print('CSV conversion finished')
    


if __name__ == "__main__":
    response = get_request(url)
    data = get_data(response)
    print(data["1091500"]["success"])
    with open(App_Details_json, 'w') as file:
        json.dump(data, file, indent=1)
    json_to_csv(App_Details_json, App_Details_csv)

