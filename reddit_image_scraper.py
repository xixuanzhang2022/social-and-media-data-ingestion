import requests
import pandas as pd
import time
import random
import os
from requests.exceptions import ConnectionError, Timeout, RequestException

CSV_FILE = "climateskeptics_submissions.csv"
OUTPUT_DIR = "./downloaded"
ERROR_LOG = "problem.pkl"
VALID_EXTENSIONS = ["jpg", "jpeg", "png", "gif"]
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}
START_DATE = "2018-01-01"
END_DATE = "2022-12-31"
MAX_RETRIES = 3
SLEEP_RANGE = (1, 6)

os.makedirs(OUTPUT_DIR, exist_ok=True)

probleml = []
problemlurl = []
typel = []

def is_image_url(url: str) -> bool:
    return any(url.lower().endswith(ext) for ext in VALID_EXTENSIONS) or "imgur.com" in url

def download_image(url: str, image_id: str):
    image_url = url if url.lower().endswith(tuple(VALID_EXTENSIONS)) else url + ".jpg"
    filepath = os.path.join(OUTPUT_DIR, f"{image_id}.jpg")

    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(image_url, headers=HEADERS, timeout=10)
            response.raise_for_status()

            with open(filepath, "wb") as f:
                f.write(response.content)
            print(f"Downloaded: {image_id}")
            time.sleep(random.uniform(*SLEEP_RANGE))  # polite delay
            return

        except (ConnectionError, Timeout, RequestException) as e:
            print(f"Failed attempt {attempt+1} for {image_id}: {e}")
            if attempt == MAX_RETRIES - 1:
                print(f"Giving up on {image_id}")
                probleml.append(image_id)
                problemlurl.append(image_url)
                typel.append(str(e))
            else:
                time.sleep(2 ** attempt)  # exponential backoff

def main():
    df = pd.read_csv(CSV_FILE)
    df["created"] = pd.to_datetime(df["created"], errors="coerce")
    df = df[(df["created"] >= START_DATE) & (df["created"] <= END_DATE)]
    df = df.dropna(subset=["url", "id"]).reset_index(drop=True)

    image_posts = df[df["url"].apply(lambda x: isinstance(x, str) and is_image_url(x))]
    print(f"Found {len(image_posts)} image posts out of {len(df)} total")

    for _, row in image_posts.iterrows():
        download_image(row["url"], str(row["id"]))

    if probleml:
        error_df = pd.DataFrame({"failed": probleml, "url": problemlurl, "type": typel})
        error_df.to_pickle(ERROR_LOG)
        print(f"Logged {len(probleml)} failed downloads to {ERROR_LOG}")

if __name__ == "__main__":
    main()
