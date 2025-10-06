#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, csv, json, argparse

# Defaults: keep everything in the current (preprocessing) directory
GROUNDTRUTH_CSV = "ground_truth.csv"                 # produced by dataset-groundtruth-bird.py
POLICY_FULL_CSV = "db_access_policies_full.csv"      # produced by access-policies-per-db-bird.py
OUT_JSONL       = "bird_acl_dataset_all.jsonl"       # unified final dataset

def load_groundtruth(path):
    rows = []
    with open(path, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            row["permit"] = int(row.get("permit", 0))
            rows.append(row)
    return rows

def load_policies_full(path):
    """
    Returns dict: dbname -> {policy_sql, schema_ddl}
    (db_id values in your CSV are plain names like 'airline', not 'bird_airline')
    """
    d = {}
    with open(path, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            db = (row["db_id"] or "").strip().lower()
            d[db] = {
                "policy_sql": row.get("access_policy_sql", "") or "",
                "schema_ddl": row.get("db_schema_ddl", "") or "",
            }
    return d

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--groundtruth", default=GROUNDTRUTH_CSV)
    ap.add_argument("--policies_full", default=POLICY_FULL_CSV)
    ap.add_argument("--out_jsonl", default=OUT_JSONL)
    ap.add_argument("--drop_nonselect_or_skip", action="store_true",
                    help="Drop rows where sqlstate == 'SKIP' (mutating or non-SELECT).")
    ap.add_argument("--only_privilege_or_permit", action="store_true",
                    help="Keep only rows that are permitted OR denied due to insufficient privilege (SQLSTATE 42501).")
    args = ap.parse_args()

    gt  = load_groundtruth(args.groundtruth)
    pol = load_policies_full(args.policies_full)

    n_in = len(gt)
    out = []

    for row in gt:
        dbname = (row.get("dbname") or "").strip().lower()
        policy = pol.get(dbname, {"policy_sql": "", "schema_ddl": ""})

        sqlstate = (row.get("sqlstate") or "").strip()
        if args.drop_nonselect_or_skip and sqlstate == "SKIP":
            continue
        if args.only_privilege_or_permit:
            if not (row["permit"] == 1 or sqlstate == "42501"):
                continue

        ex = {
            # identity
            "split": row.get("split", ""),
            "db_id": (row.get("db_id") or "").strip(),
            "dbname": dbname,
            "user": row.get("role", ""),          # rename 'role' -> 'user' for clarity

            # question + SQL
            "qid": row.get("qid", ""),
            "question": row.get("question", "") or "",
            "sql": row.get("sql_original", "") or "",
            "sql_wrapped": row.get("sql_wrapped", "") or "",

            # label
            "decision": "PERMIT" if row["permit"] == 1 else "DENY",
            "permit": bool(row["permit"]),
            "sqlstate": sqlstate,
            "error": row.get("error", "") or "",

            # context
            "evidence": row.get("evidence", "") or "",
            "policy_sql": policy["policy_sql"],
            "schema_ddl": policy["schema_ddl"],
        }
        out.append(ex)

    # Save next to the script (preprocessing folder)
    os.makedirs(os.path.dirname(args.out_jsonl) or ".", exist_ok=True)
    with open(args.out_jsonl, "w", encoding="utf-8") as f:
        for ex in out:
            f.write(json.dumps(ex, ensure_ascii=False) + "\n")

    # Summary
    permits = sum(1 for x in out if x["permit"])
    denies = len(out) - permits
    priv_denies = sum(1 for x in out if (not x["permit"]) and x["sqlstate"] == "42501")
    print(f"✅ Wrote {args.out_jsonl}")
    print(f"ℹ️ Input rows: {n_in}  →  Output rows: {len(out)}")
    print(f"   Permitted: {permits} | Denied: {denies} | Denied (42501): {priv_denies}")

if __name__ == "__main__":
    main()

