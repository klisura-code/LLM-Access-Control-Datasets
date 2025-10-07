import json
import csv
import os

# Set your data directory path
DATA_DIR = os.getenv("DATA_DIR", os.path.expanduser("~/path/to/spider/data"))
TRAIN_FILE = os.path.join(DATA_DIR, "train_spider.json")
DEV_FILE = os.path.join(DATA_DIR, "dev.json")
OUTPUT_FILE = os.path.join(DATA_DIR, "spider_nl_sql_pairs.csv")

# Load data from JSON files
with open(TRAIN_FILE, "r", encoding="utf-8") as f:
    train_data = json.load(f)

with open(DEV_FILE, "r", encoding="utf-8") as f:
    dev_data = json.load(f)

# Combine datasets
all_data = train_data + dev_data

# Write to CSV
with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["question", "sql", "db_id"])  # header

    for item in all_data:
        question = item["question"]
        sql = item["query"]
        db_id = item["db_id"]
        writer.writerow([question, sql, db_id])

print(f"✅ Extracted {len(all_data)} NL-SQL pairs to: {OUTPUT_FILE}")
