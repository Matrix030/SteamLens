import json
import os
import time


def get_directory(app_id):
    return f'../app_details_json/{app_id}_data/Review_Details{app_id}.json'

def all_app_ids(dirs):
    return [str(d.split('_')[0]) for d in dirs]

def get_dirs():
    dirs = []
    for root, subdirs, files in os.walk('../app_details_json', topdown=True):
        if root == '../app_details_json':  # Only collect immediate subdirs
            subdirs.sort(key=lambda name: int(name.split('_')[0]))
            dirs = subdirs
            break
    return dirs

def get_json(app_id):
    with open(get_directory(app_id), 'r') as file:
        info = json.load(file)
    return info

def review_length(info, app_id):
    return len(info[app_id]['data']['review_stats']['reviews'])

if __name__ == "__main__":
    total_reviews_all = 0
    dirs = get_dirs()
    app_ids = all_app_ids(dirs)
    start_time = time.time()
    for app_id in app_ids:
        try:
            info = get_json(app_id)
            total_reviews = review_length(info, app_id)
            with open("review_lengths.txt", 'a') as file:
                file.write(f'{app_id}: {total_reviews}\n')
            total_reviews_all += total_reviews
        except FileNotFoundError:
            print(f"File for appId {app_id} not found, skipping.")
        except KeyError:
            print(f"Review data missing for appId {app_id}, skipping.")
    end_time = time.time()
    total_time = end_time - start_time
    print(f"Total Reviews Stored: {total_reviews_all}")
    print(f'Time Taken: {total_time:.2f} seconds')
