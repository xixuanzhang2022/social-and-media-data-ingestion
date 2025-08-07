import os
import time
import json
import requests
import pandas as pd
from collections import defaultdict
from datetime import datetime

BEARER_TOKEN = os.getenv("BEARER_TOKEN")
SEARCH_URL = "https://api.twitter.com/2/tweets/search/all"
START_TIME = "2011-01-01T00:00:00Z"
END_TIME = "2022-01-01T00:00:00Z"
MAX_RESULTS = '500'
LANG = "de"

QUERY_EXP = """xxx"""# Query for conspiracy-related expressions in German context
QUERY = f"{QUERY_EXP} lang:{LANG} -is:retweet"

OUTDIR = "/Users/xixuanzhang/Documents/Neovex/Datenerhebung/Twitter"
os.makedirs(OUTDIR, exist_ok=True)

def create_headers(token):
    return {"Authorization": f"Bearer {token}"}

def connect_to_endpoint(params, headers):
    response = requests.get(SEARCH_URL, headers=headers, params=params)
    print(f"Request status: {response.status_code}")
    if response.status_code != 200:
        raise Exception(f"API Error {response.status_code}: {response.text}")
    return response.json()

def process_response(json_data):
    tweets = []
    users = defaultdict(str)

    for tweet in json_data.get("data", []):
        reftype, refid = "", ""
        if "referenced_tweets" in tweet:
            ref = tweet["referenced_tweets"][0]
            reftype, refid = ref.get("type", ""), ref.get("id", "")
        tweets.append([
            tweet["created_at"],
            tweet["id"],
            tweet["author_id"],
            tweet["text"],
            reftype,
            refid
        ])

    for user in json_data.get("includes", {}).get("users", []):
        users[user["id"]] = user["username"]

    return tweets, users

def save_data(tweets, users, newest_time):
    tweet_df = pd.DataFrame(tweets, columns=["time", "id", "author_id", "text", "ref_type", "ref_id"])
    users_df = pd.DataFrame(list(users.items()), columns=["id", "username"])
    pd.DataFrame([newest_time], columns=["newest"]).to_csv(f"{OUTDIR}/tweet_time_exp{LANG}.csv", index=False)
    tweet_df.to_csv(f"{OUTDIR}/tweet_all_exp{LANG}.csv", mode="a", header=False, index=False)
    users_df.to_csv(f"{OUTDIR}/users_all_exp{LANG}.csv", mode="a", header=False, index=False)

def main():
    headers = create_headers(BEARER_TOKEN)
    next_token = None
    round_count = 0

    while True:
        params = {
            "query": QUERY,
            "max_results": MAX_RESULTS,
            "expansions": "author_id",
            "tweet.fields": "id,created_at,author_id,text,referenced_tweets",
            "user.fields": "username",
            "start_time": START_TIME,
            "end_time": END_TIME,
        }
        if next_token:
            params["next_token"] = next_token

        try:
            data = connect_to_endpoint(params, headers)
        except Exception as e:
            print(f"Error during request: {e}")
            break

        tweets, users = process_response(data)
        if not tweets:
            print("No tweets found in this page.")
            break

        newest = tweets[-1][0]
        save_data(tweets, users, newest)
        round_count += 1
        print(f"Page {round_count} processed, {len(tweets)} tweets saved")

        meta = data.get("meta", {})
        if "next_token" in meta:
            next_token = meta["next_token"]
            pd.DataFrame([next_token]).to_csv("ns_df.csv", mode="a", header=False, index=False)
            time.sleep(3)
        else:
            print("Reached end of results")
            break

if __name__ == "__main__":
    main()
