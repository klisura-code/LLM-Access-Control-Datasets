import pandas as pd
import json
import random
from pathlib import Path
from collections import defaultdict
import numpy as np

# Reload files after kernel reset
users_df = pd.read_csv("user_with_read.csv")

# Load Spider dev.json
with open("/home/dorde/Desktop/Access-Control-Project/spider/train_spider.json", "r") as f:
    spider_dev = json.load(f)

# Group Spider entries by db_id
entries_by_schema = defaultdict(list)
for item in spider_dev:
    db_id = item["db_id"]
    entries_by_schema[db_id].append((item["question"], item["query"]))

# Assign one (question, SQL) pair per user
assigned = []
for _, row in users_df.iterrows():
    username = row["username"]
    schema = row["database"]
    read_access = row["read_access"]

    schema_entries = entries_by_schema.get(schema, [])
    if not schema_entries:
        continue  

    question, query = random.choice(schema_entries)
    assigned.append({
        "username": username,
        "schema": schema,
        "read_access": read_access,
        "question": question,
        "sql_query": query
    })

# Convert to DataFrame and show
assigned_df = pd.DataFrame(assigned)
assigned_df_path = "assigned_user_queries.csv"
assigned_df.to_csv(assigned_df_path, index=False)
