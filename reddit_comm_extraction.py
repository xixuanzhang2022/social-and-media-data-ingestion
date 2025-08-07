import zstandard as zstd
import os
import json
import sys
import csv
from datetime import datetime
import logging
import pandas as pd

FIELDS = ["body", "author", "subreddit", "created_utc", "id", "link_id", "parent_id"]
SUBREDDIT_FILE = '/scratch/zhangxixuan/tmp/reddit/parser/final_subreddit_en.csv'
KEYWORDS_FILE = '/scratch/zhangxixuan/tmp/reddit/parser/keywords_complete_deduplicated.csv'
INPUT_DIR = "/scratch/zhangxixuan/tmp/reddit/comments/"
OUTPUT_DIR = "/scratch/zhangxixuan/tmp/reddit/parser_r1/"

log = logging.getLogger("reddit_comment_parser")
log.setLevel(logging.INFO)
log.addHandler(logging.StreamHandler())

df_subr = pd.read_csv(SUBREDDIT_FILE)
df_keywords = pd.read_csv(KEYWORDS_FILE)

subreddits = set(df_subr["subr"].str.lower().dropna())
keywords = set(df_keywords["keyword_ger"].str.lower().dropna())

def read_and_decode(reader, chunk_size, max_window_size, previous_chunk=None, bytes_read=0):
    chunk = reader.read(chunk_size)
    bytes_read += chunk_size
    if previous_chunk:
        chunk = previous_chunk + chunk
    try:
        return chunk.decode()
    except UnicodeDecodeError:
        if bytes_read > max_window_size:
            raise
        return read_and_decode(reader, chunk_size, max_window_size, chunk, bytes_read)

def read_lines_zst(filepath):
    with open(filepath, 'rb') as fh:
        buffer = ''
        reader = zstd.ZstdDecompressor(max_window_size=2**31).stream_reader(fh)
        while True:
            chunk = read_and_decode(reader, 2**27, 2**30)
            if not chunk:
                break
            lines = (buffer + chunk).split('\n')
            for line in lines[:-1]:
                yield line, fh.tell()
            buffer = lines[-1]
        reader.close()

def comment_matches(obj):
    subreddit = obj.get("subreddit", "").lower()
    if subreddit not in subreddits:
        return False
    body = obj.get("body", "").lower()
    return any(kw in body for kw in keywords)

def main(input_filename):
    input_path = os.path.join(INPUT_DIR, input_filename)
    output_name = f"en_{input_filename.replace('.zst', '.csv')}"
    output_path = os.path.join(OUTPUT_DIR, output_name)
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    file_size = os.path.getsize(input_path)
    total, matched, bad = 0, 0, 0
    created = None

    with open(output_path, "w", encoding="utf-8", newline="") as fout:
        writer = csv.writer(fout)
        writer.writerow(FIELDS)

        for line, file_bytes_processed in read_lines_zst(input_path):
            total += 1
            if total % 100_000 == 0:
                log.info(f"{created} | Total: {total:,} | Matched: {matched:,} | Bad: {bad:,} | {(file_bytes_processed / file_size):.1%}")

            try:
                obj = json.loads(line)
                created = datetime.utcfromtimestamp(int(obj["created_utc"]))

                if comment_matches(obj):
                    row = [str(obj.get(f, "")).encode("utf-8", errors="replace").decode() for f in FIELDS]
                    writer.writerow(row)
                    matched += 1

            except (json.JSONDecodeError, KeyError, ValueError):
                bad += 1
                continue

    log.info(f"Completed {input_filename} | Total: {total:,} | Matched: {matched:,} | Bad: {bad:,}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python extract_reddit_comments.py <comments_file.zst>")
        sys.exit(1)
    main(sys.argv[1])
