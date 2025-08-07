import zstandard as zstd
import os
import json
import sys
import csv
from datetime import datetime
import logging
import pandas as pd

FIELDS = ["title", "selftext", "author", "subreddit", "created_utc", "permalink"]
SUBREDDITS_FILE = '/scratch/zhangxixuan/tmp/reddit/parser/final_subreddit_en.csv'
KEYWORDS_FILE = '/scratch/zhangxixuan/tmp/reddit/parser/keywords_complete_deduplicated.csv'
INPUT_DIR = "/scratch/zhangxixuan/tmp/reddit/submissions/"
OUTPUT_DIR = "/scratch/zhangxixuan/tmp/reddit/parser_r1/"

log = logging.getLogger("reddit_zst_parser")
log.setLevel(logging.INFO)
log.addHandler(logging.StreamHandler())

df_subr = pd.read_csv(SUBREDDITS_FILE)
df_keywords = pd.read_csv(KEYWORDS_FILE)

subreddits = set(df_subr["subr"].str.lower().dropna().tolist())
keywords = set(df_keywords["keyword_ger"].str.lower().dropna().tolist())

def read_and_decode(reader, chunk_size, max_window_size, previous_chunk=None, bytes_read=0):
    chunk = reader.read(chunk_size)
    bytes_read += chunk_size
    if previous_chunk is not None:
        chunk = previous_chunk + chunk
    try:
        return chunk.decode()
    except UnicodeDecodeError:
        if bytes_read > max_window_size:
            raise UnicodeError(f"Unable to decode after reading {bytes_read:,} bytes")
        return read_and_decode(reader, chunk_size, max_window_size, chunk, bytes_read)

def read_lines_zst(file_path):
    with open(file_path, 'rb') as fh:
        buffer = ''
        reader = zstd.ZstdDecompressor(max_window_size=2**31).stream_reader(fh)
        while True:
            chunk = read_and_decode(reader, 2**27, 2**30)
            if not chunk:
                break
            lines = (buffer + chunk).split("\n")
            for line in lines[:-1]:
                yield line, fh.tell()
            buffer = lines[-1]
        reader.close()

def matches(obj):
    subreddit = obj.get("subreddit", "").lower()
    if subreddit not in subreddits:
        return False

    text = obj.get("selftext", "").lower()
    title = obj.get("title", "").lower()
    return any(k in title or k in text for k in keywords)

def process_file(input_file_name):
    input_path = os.path.join(INPUT_DIR, input_file_name)
    output_file_name = "en_" + input_file_name.replace(".zst", ".csv")
    output_path = os.path.join(OUTPUT_DIR, output_file_name)

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    log.info(f"Processing {input_file_name}")
    file_size = os.stat(input_path).st_size

    matched_lines = bad_lines = total_lines = 0
    created = None

    with open(output_path, "w", encoding='utf-8', newline='') as out_f:
        writer = csv.writer(out_f)
        writer.writerow(FIELDS)

        for line, file_bytes_processed in read_lines_zst(input_path):
            total_lines += 1
            if total_lines % 100_000 == 0:
                log.info(f"{created} | Total: {total_lines:,} | Matched: {matched_lines:,} | Bad: {bad_lines:,} | {file_bytes_processed/file_size:.1%}")

            try:
                obj = json.loads(line)
                created = datetime.utcfromtimestamp(int(obj["created_utc"]))

                if matches(obj):
                    writer.writerow([str(obj.get(field, "")).encode("utf-8", errors="replace").decode() for field in FIELDS])
                    matched_lines += 1

            except (json.JSONDecodeError, KeyError, ValueError) as e:
                bad_lines += 1
                continue

    log.info(f"Done: {input_file_name} | Total: {total_lines:,} | Matched: {matched_lines:,} | Bad lines: {bad_lines:,}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python extract_reddit_submissions.py <reddit_zst_file>")
        sys.exit(1)

    input_file_name = sys.argv[1]
    process_file(input_file_name)
