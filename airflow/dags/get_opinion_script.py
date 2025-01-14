import requests
import os
import json
import calendar
import pandas as pd
from datetime import datetime, timezone
from transformers import pipeline

def scrape_subreddit(subreddit, search_terms, start_year, end_year):
    type = ["submission", "comment"]

    for type in type:
        for search_term in search_terms:
            for year in range(start_year, end_year + 1):
                for month in range(1, 12 + 1):
                    last_day = calendar.monthrange(year, month)[1]

                    date_debut = datetime(year, month, 1, 0, 0, 0)
                    date_fin = datetime(year, month, last_day, 23, 59, 59)
                    sort = "asc"

                    response = requests.get(
                        f"https://api.pullpush.io/reddit/{type}/search?&subreddit={subreddit}&since={int(date_debut.timestamp())}&until={int(date_fin.timestamp())}&q={search_term}&size=100&sort={sort}"
                    )

                    if response.status_code == 200:
                        data = response.json()
                        file = f"/opt/airflow/datasets/pullpush_files_by_month/{search_term}/{search_term}_{type}_{year}_{month}.json"
                        with open(file, "w", encoding="utf-8") as f:
                            json.dump(data, f, ensure_ascii=False, indent=4)
                        print(f"Data saved to {file}")
                    else:
                        print(f"Error: {response.status_code}, {response.text}")

    print("End of scrape")

def read_submission(path):
    if "submission" not in path:
        print("This file contains comments, not submissions.")
        print("Please use the method 'read_comment()'")
        return
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    post_details = []

    for post in data.get("data", []):
        body = post.get("selftext", "[No body]")
        title = post.get("title", "[No title]")
        if len(body) < 1:
            body = "[No body]"
        created_utc = post.get("created_utc", None)
        if created_utc != "[No date]":
            post_date = datetime.fromtimestamp(created_utc, tz=timezone.utc).strftime(
                "%Y-%m-%d"
            )
        else:
            post_date = "[No date]"

        post_details.append({"text": f"{title} | {body}", "date": post_date})

    return post_details


def read_comment(path):
    if "comment" not in path:
        print("This file contains submissions, not comments.")
        print("Please use the method 'read_submission()'")
        return
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if isinstance(data, list):
        return []
    post_details = []

    for post in data.get("data", []):
        body = post.get("body", "[No body]")
        created_utc = post.get("created_utc", None)
        if created_utc:
            created_utc = int(created_utc)
            post_date = datetime.fromtimestamp(created_utc, tz=timezone.utc).strftime(
                "%Y-%m-%d"
            )
        else:
            post_date = "[No date]"
        post_details.append({"text": f"{body}", "date": post_date})

    return post_details

def count_sentiments_in_json(json_file):
    with open(json_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    if isinstance(data, dict) and "data" in data and not data["data"]:
        return None
    sentiment_counts = {"negative": 0, "positive": 0, "neutral": 0}
    if isinstance(data, list):
        for entry in data:
            sentiment = entry.get("sentiment", "").lower()
            if sentiment in sentiment_counts:
                sentiment_counts[sentiment] += 1
    return sentiment_counts

def get_sentiment(text):
    sentiment_analyzer = pipeline(
        "sentiment-analysis", model="nlptown/bert-base-multilingual-uncased-sentiment"
    )

    results = sentiment_analyzer(text[:512])
    scores = results[0]["label"]

    if "4" in scores or "5" in scores:
        return "positive"
    elif "2" in scores or "1" in scores:
        return "negative"
    else:
        return "neutral"
    
def check_sentiment_in_file():
    try:
        with open("/opt/airflow/dags/opinion_data/pullpush_files_by_month/kohle/kohle_comment_2019_3.json", 'r', encoding='utf-8') as file:
            data = json.load(file)
        if isinstance(data, list):
            return any('sentiment' in item for item in data)
        elif isinstance(data, dict):
            return 'sentiment' in data
        else:
            return False
    except Exception:
                    print(f"File not found")

def analyze_sentiment_in_file(input_path):
    type = "submission" if "submission" in input_path else "comment"
    if type == "submission":
        data = read_submission(input_path)
        for item in data:
            if item["text"] == "[No title]":
                text = ""
            else:
                text = item["text"]
            if "[No title]" in text:
                text = text.split("|")[1]
                if "[No body]" in text:
                    text = ""
            elif "[No body]" in text:
                text = text.split("|")[0]
            item["sentiment"] = get_sentiment(text)
            with open(input_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=4)
    else:
        data = read_comment(input_path)
        for item in data:
            if item["text"] == "[No body]":
                text = ""
            else:
                text = item["text"]
            item["sentiment"] = get_sentiment(text)
            with open(input_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=4)

    return

def process_sentiment_all_files_in_directory(base_dir):
    for root, dirs, files in os.walk(base_dir):
        for file in files:
            if file.endswith(".json"):
                file_path = os.path.join(root, file)
                print(f"Processing file: {file_path}")
                try:
                    analyze_sentiment_in_file(file_path)
                except Exception as e:
                    print(f"Error processing file {file_path}: {e}")

def process_opinion_data(directory_path,output_csv_path):
    results = []
    for root, _, files in os.walk(directory_path):
        for file in files:
            if file.endswith(".json"):
                filename_parts = file.split("_")
                if len(filename_parts) >= 4:
                    typeenergie = filename_parts[0]
                    year_month = f"{filename_parts[2]}-{filename_parts[3].replace('.json', '')}"
                    json_file = os.path.join(root, file)
                    sentiment_counts = count_sentiments_in_json(json_file)
                    if sentiment_counts:
                        results.append({
                            "year-month": year_month,
                            "typeenergie": typeenergie,
                            "negative": sentiment_counts["negative"],
                            "positif": sentiment_counts["positive"],
                            "neutre": sentiment_counts["neutral"]
                        })

    df = pd.DataFrame(results)

    df.to_csv(output_csv_path, index=False)

def process_all_files_in_directory(base_dir):
    if check_sentiment_in_file():
        return
    for root, dirs, files in os.walk(base_dir):
        for file in files:
            if file.endswith(".json"):
                file_path = os.path.join(root, file)
                print(f"Processing file: {file_path}")
                try:
                    analyze_sentiment_in_file(file_path)
                except Exception as e:
                    print(f"Error processing file {file_path}: {e}")