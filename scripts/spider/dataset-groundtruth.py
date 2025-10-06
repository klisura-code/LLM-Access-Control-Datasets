#!/usr/bin/env python3
"""
Run every Spider query as each of the four per-database roles and
record the outcome.

Input : spider_nl_sql_pairs.csv   (question, sql, db_id)
Output: spider_nl_sql_pairs_with_results.csv
         question | sql | db_id | User_1_result | User_2_result | User_3_result | User_4_result
"""

import csv
import json
import os
import sys
import time
import psycopg2
from psycopg2 import ProgrammingError, OperationalError, errors

# ── PostgreSQL super-user credentials ──────────────────────────────────────────
PG_USER     = "dorde"
PG_PASSWORD = "project123"
PG_HOST     = "localhost"
PG_PORT     = 5432
# ── paths ──────────────────────────────────────────────────────────────────────
DATA_DIR        = "/home/dorde/Desktop/Access-Control-Project/data/spider"
INPUT_CSV       = os.path.join(DATA_DIR, "spider_nl_sql_pairs.csv")
OUTPUT_CSV      = os.path.join(DATA_DIR, "dataset-groundtruth.csv")
# ── how many rows to store when a query succeeds (None ⇒ ALL / can be huge!) ──
ROW_LIMIT_TO_STORE = 5

ROLE_SUFFIXES = ["User_1", "User_2", "User_3", "User_4"]


# ───────────────────────────────────────────────────────────────────────────────
def connect_as_admin(db_name: str):
    """Return a psycopg2 connection *as super-user* to the given db."""
    return psycopg2.connect(
        dbname=db_name,
        user=PG_USER,
        password=PG_PASSWORD,
        host=PG_HOST,
        port=PG_PORT,
    )


def stringify_result(cur):
    """Return a readable, JSON-style preview of the SELECT result."""
    try:
        rows = cur.fetchmany(ROW_LIMIT_TO_STORE) if ROW_LIMIT_TO_STORE else cur.fetchall()
        return json.dumps(rows, default=str)
    except ProgrammingError:  # no result (e.g. INSERT)
        return "OK-NO-ROWS"


def run_query_with_role(conn, sql: str, role_name: str) -> str:
    """
    Execute a single SQL statement while SET ROLE -ed to `role_name`.

    Returns a string that either contains rows (on success) or the error text.
    """
    cur = conn.cursor()
    try:
        cur.execute(f'SET ROLE "{role_name}";')
        cur.execute(sql)
        result_str = stringify_result(cur)
        cur.execute("RESET ROLE;")
        return result_str
    except Exception as e:  # capture & reset role before propagating
        try:
            cur.execute("RESET ROLE;")
        except Exception:
            pass
        return f"ERROR: {e}"
    finally:
        cur.close()


# ───────────────────────────────────────────────────────────────────────────────
def main():
    if not os.path.isfile(INPUT_CSV):
        sys.exit(f"❌  Could not find {INPUT_CSV}")

    # read the Spider pairs -----------------------------------------------------
    with open(INPUT_CSV, newline="", encoding="utf-8") as f_in:
        reader = csv.DictReader(f_in)
        rows = list(reader)

    total = len(rows)
    print(f"🔍 Loaded {total:,} question-SQL pairs.")

    # prepare output CSV --------------------------------------------------------
    out_headers = reader.fieldnames + [f"{role}_result" for role in ROLE_SUFFIXES]
    f_out = open(OUTPUT_CSV, "w", newline="", encoding="utf-8")
    writer = csv.DictWriter(f_out, fieldnames=out_headers)
    writer.writeheader()

    # process row by row --------------------------------------------------------
    current_db = None
    conn = None
    processed = 0
    try:
        for r in rows:
            db_id   = r["db_id"]
            sql     = r["sql"]

            # open a new admin connection only when db changes
            if current_db != db_id:
                if conn:
                    conn.close()
                try:
                    conn = connect_as_admin(db_id)
                    conn.autocommit = True
                except OperationalError as e:
                    # if DB missing, mark all four results as error and continue
                    err_txt = f"ERROR: cannot connect: {e}"
                    for suffix in ROLE_SUFFIXES:
                        r[f"{suffix}_result"] = err_txt
                    writer.writerow(r)
                    current_db = None
                    processed += 1
                    continue
                current_db = db_id

            # run SQL for each of the four roles
            for suffix in ROLE_SUFFIXES:
                role_name = f"{db_id}_{suffix}"
                outcome   = run_query_with_role(conn, sql, role_name)
                r[f"{suffix}_result"] = outcome

            writer.writerow(r)
            processed += 1

            if processed % 500 == 0:
                print(f"   …{processed:,}/{total:,} done")

    finally:
        if conn:
            conn.close()
        f_out.close()

    print(f"✅ Finished. Results saved to {OUTPUT_CSV}.")


# ───────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    t0 = time.time()
    main()
    print(f"Elapsed: {time.time() - t0:,.1f} s")
