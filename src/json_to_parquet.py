import json
import os
import time

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

def get_directory(app_id):
    path = f'../App_Details/{app_id}_data/Review_Details{app_id}.json'
    print(f"[DEBUG] get_directory: app_id={app_id} → {path}")
    return path

def all_app_ids(dirs):
    ids = [str(d.split('_')[0]) for d in dirs]
    print(f"[DEBUG] all_app_ids: dirs={dirs[:5]}... → app_ids={ids[:5]}...")
    return ids

def get_dirs():
    print("[DEBUG] Scanning '../App_Details' for subdirectories...")
    for root, subdirs, files in os.walk('../App_Details', topdown=True):
        if root == '../App_Details':  # Only collect immediate subdirs
            subdirs.sort(key=lambda name: int(name.split('_')[0]))
            print(f"[DEBUG] Found subdirs: {subdirs[:5]}...")
            return subdirs
    print("[DEBUG] No subdirectories found.")
    return []

def get_json(app_id):
    path = get_directory(app_id)
    print(f"[DEBUG] Loading JSON for app_id={app_id} from {path}")
    with open(path, 'r') as file:
        info = json.load(file)
    print(f"[DEBUG] JSON loaded: keys={list(info.keys())}")
    return info

def review_length(info, app_id):
    try:
        length = len(info[app_id]['data']['review_stats']['reviews'])
        print(f"[DEBUG] review_length: app_id={app_id} → {length}")
        return length
    except Exception as e:
        print(f"[ERROR] review_length failed for app_id={app_id}: {e}")
        raise

def convert_json_to_parquet(info, app_id, output_dir="parquet_output"):
    print(f"[DEBUG] convert_json_to_parquet: start for app_id={app_id}")
    data = info[app_id]['data']
    reviews = data.get('review_stats', {}).get('reviews', [])
    print(f"[DEBUG] Number of reviews to process: {len(reviews)}")
    rows = []

    for i, review in enumerate(reviews, start=1):
        if i % 1000 == 0:
            print(f"[DEBUG] Processing review #{i}")
        entry = {
            'steam_appid':           data.get('steam_appid'),
            'required_age':          data.get('required_age'),
            'is_free':               data.get('is_free'),
            'controller_support':    data.get('controller_support'),
            'detailed_description':  data.get('detailed_description'),
            'about_the_game':        data.get('about_the_game'),
            'short_description':     data.get('short_description'),
            'price_overview':        data.get('price_overview', {}).get('final_formatted'),
            'metacritic_score':      data.get('metacritic', {}).get('score'),
            'categories':            [c.get('description') for c in data.get('categories', [])],
            'genres':                [g.get('description') for g in data.get('genres', [])],
            'recommendations':       data.get('recommendations', {}).get('total'),
            'achievements':          data.get('achievements', {}).get('total'),
            'release_coming_soon':   data.get('release_date', {}).get('coming_soon'),
            'release_date':          data.get('release_date', {}).get('date'),
            'review_language':       review.get('language'),
            'review':                review.get('review'),
            'voted_up':              review.get('voted_up'),
            'votes_up':              review.get('votes_up'),
            'votes_funny':           review.get('votes_funny'),
            'weighted_vote_score':   float(review.get('weighted_vote_score')) 
                                      if review.get('weighted_vote_score') is not None 
                                      else None
        }
        rows.append(entry)

    print(f"[DEBUG] Building DataFrame with {len(rows)} rows")
    df = pd.DataFrame(rows)
    df = df.where(pd.notnull(df), None)  # Replace NaN with None

    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, f"{app_id}.parquet")
    print(f"[DEBUG] Writing Parquet to {output_file}")

    table = pa.Table.from_pandas(df)
    pq.write_table(table, output_file)

    print(f"[DEBUG] Saved {len(rows)} reviews to {output_file}")

if __name__ == "__main__":
    print("[DEBUG] Script started")
    total_reviews_all = 0

    dirs = get_dirs()
    app_ids = all_app_ids(dirs)

    start_time = time.time()
    for app_id in app_ids:
        print(f"\n[DEBUG] Processing app_id={app_id}")
        try:
            info = get_json(app_id)
            total_reviews = review_length(info, app_id)
            with open("PARQUET_length.txt", 'a') as file:
                file.write(f'Total reviews for appId {app_id}: {total_reviews}\n')
            total_reviews_all += total_reviews
            convert_json_to_parquet(info, app_id)
            # Optional: convert each to Parquet immediately
            # convert_json_to_parquet(info, app_id)

        except FileNotFoundError:
            print(f"[WARN] File for appId {app_id} not found, skipping.")
        except KeyError:
            print(f"[WARN] Review data missing for appId {app_id}, skipping.")
        except Exception as e:
            print(f"[ERROR] Unexpected error for appId {app_id}: {e}")

    end_time = time.time()
    total_time = end_time - start_time

    print(f"\n[DEBUG] Total Reviews Stored: {total_reviews_all}")
    print(f"[DEBUG] Time Taken: {total_time:.2f} seconds")
