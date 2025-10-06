#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import json, csv, os, sys, re, argparse

# ---------- helpers ----------
def load_json(path):
    if not os.path.isfile(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def normalize_item(item, default_qid):
    q = item.get("question") or item.get("utterance") or ""
    sql = item.get("query") or item.get("SQL") or item.get("sql") or ""
    dbid = item.get("db_id") or item.get("database_id") or item.get("db") or ""
    evidence = item.get("evidence") or item.get("notes") or ""
    qid = str(item.get("question_id", default_qid))
    return {"question": q, "sql": sql, "db_id": dbid, "evidence": evidence, "qid": qid}

def load_gold_sql_map(sql_path):
    if not os.path.isfile(sql_path):
        return {}
    m = {}
    with open(sql_path, "r", encoding="utf-8") as f:
        cur_db, cur_qid, buf = None, None, []
        for line in f:
            cm = re.match(r"\s*--\s*db:\s*([^\s]+)\s+qid:\s*([^\s]+)\s*$", line, re.I)
            if cm:
                if cur_db and cur_qid and buf:
                    m[(cur_db, cur_qid)] = "".join(buf).strip().rstrip(";")
                cur_db, cur_qid, buf = cm.group(1), cm.group(2), []
            else:
                if cur_db and cur_qid is not None:
                    buf.append(line)
        if cur_db and cur_qid and buf:
            m[(cur_db, cur_qid)] = "".join(buf).strip().rstrip(";")
    return m

def write_jsonl(path, rows):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

# ---------- main ----------
def main():
    ap = argparse.ArgumentParser()
    BIRD_DIR_DEFAULT = os.path.expanduser("~/Desktop/Access-Control-Project/BIRD/bird")
    ap.add_argument("--bird_dir", default=BIRD_DIR_DEFAULT)

    # explicit paths allowed; sensible defaults otherwise
    ap.add_argument("--dev_json", default=None)
    ap.add_argument("--dev_sql",  default=None)
    ap.add_argument("--train_json", default=None)
    ap.add_argument("--train_sql",  default=None)
    ap.add_argument("--tied_append", default=None)

    ap.add_argument("--out_jsonl", default="../data/bird_questions_sql_all.jsonl",
                    help="Unified JSONL (dev+train) with a 'split' field")
    ap.add_argument("--also_csv", action="store_true",
                    help="Also write a CSV summary for convenience")
    ap.add_argument("--out_csv", default="../data/questions_sqls.csv")
    args = ap.parse_args()

    bird_dir = args.bird_dir
    dev_json   = args.dev_json   or os.path.join(bird_dir, "dev.json")
    dev_sql    = args.dev_sql    or os.path.join(bird_dir, "dev.sql")
    train_json = args.train_json or os.path.join(bird_dir, "train.json")
    train_sql  = args.train_sql  or os.path.join(bird_dir, "train_gold.sql")
    tied_path  = args.tied_append or os.path.join(bird_dir, "dev_tied_append.json")

    dev   = load_json(dev_json)
    train = load_json(train_json)
    tied  = load_json(tied_path)

    # optional evidence index for dev
    tied_index = {}
    if isinstance(tied, list):
        for i, item in enumerate(tied):
            tnorm = normalize_item(item, default_qid=i)
            key = (tnorm["question"], tnorm["db_id"])
            if key not in tied_index:
                tied_index[key] = tnorm.get("evidence", "")

    dev_gold   = load_gold_sql_map(dev_sql)
    train_gold = load_gold_sql_map(train_sql)

    unified = []         # JSONL rows with 'split'
    summary_rows = []    # optional CSV

    # DEV (include all)
    if isinstance(dev, list) and dev:
        for i, item in enumerate(dev):
            norm = normalize_item(item, default_qid=i)
            key = (norm["question"], norm["db_id"])
            if key in tied_index and not norm["evidence"]:
                norm["evidence"] = tied_index[key]
            if not norm["sql"]:
                norm["sql"] = dev_gold.get((norm["db_id"], norm["qid"]), "")
            unified.append({"split":"dev", **norm})
            summary_rows.append({"split":"dev", **norm})

    # TRAIN (include all)
    if isinstance(train, list) and train:
        for i, item in enumerate(train):
            norm = normalize_item(item, default_qid=i)
            if not norm["sql"]:
                norm["sql"] = train_gold.get((norm["db_id"], norm["qid"]), "")
            unified.append({"split":"train", **norm})
            summary_rows.append({"split":"train", **norm})

    if not unified:
        print("❌ No data found. Check dev/train JSON and SQL maps.", file=sys.stderr)
        sys.exit(1)

    # Write unified JSONL
    write_jsonl(args.out_jsonl, unified)

    # Optional CSV summary
    if args.also_csv:
        os.makedirs(os.path.dirname(args.out_csv) or ".", exist_ok=True)
        with open(args.out_csv, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["split","db_id","qid","question","gold_sql","evidence"])
            for r in summary_rows:
                w.writerow([r["split"], r["db_id"], r["qid"], r["question"], r["sql"], r["evidence"]])

    print(f"✅ Wrote unified JSONL → {args.out_jsonl}  (rows: {len(unified)})")
    if args.also_csv:
        print(f"   and CSV summary   → {args.out_csv}      (rows: {len(summary_rows)})")

    dbs = sorted({r['db_id'] for r in unified})
    print(f"ℹ️  Unique DBs in unified file: {len(dbs)} → {dbs[:20]}{' ...' if len(dbs) > 20 else ''}")

if __name__ == "__main__":
    main()

