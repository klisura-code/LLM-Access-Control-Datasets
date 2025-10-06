#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, csv, re, psycopg2
from psycopg2 import sql

# ── Connection (BIRD stack) ──────────────────────────────────────────────────
PG_USER = "dorde"
PG_PASSWORD = "project123"
PG_HOST = "localhost"
PG_PORT = 5433

# ── Paths (run from preprocessing folder) ────────────────────────────────────
PAIRS_CSV = "questions_sqls.csv"   # input produced by extractor (in this folder)
OUT_CSV   = "ground_truth.csv"     # output written here too

# ── Roles per DB ─────────────────────────────────────────────────────────────
ROLE_SUFFIXES = ["User_1", "User_2", "User_3", "User_4"]

# ── Helpers ──────────────────────────────────────────────────────────────────
MUTATING_PAT = re.compile(
    r"^\s*(INSERT|UPDATE|DELETE|REPLACE|CREATE|DROP|ALTER|TRUNCATE|MERGE|GRANT|REVOKE)\b",
    re.IGNORECASE | re.DOTALL
)
SELECT_PAT   = re.compile(r"^\s*SELECT\b", re.IGNORECASE | re.DOTALL)

BACKTICK_IDENT = re.compile(r"`([^`]+)`")  # `Identifier` → "identifier"

def normalize_dbname(db_id: str) -> str:
    # DBs are named exactly as BIRD db_id (lowercased), no 'bird_' prefix.
    return (db_id or "").strip().lower()

def is_mutating(sql_text: str) -> bool:
    return bool(MUTATING_PAT.match(sql_text or ""))

def is_select(sql_text: str) -> bool:
    return bool(SELECT_PAT.match(sql_text or ""))

def normalize_sql_for_postgres(sql_text: str) -> str:
    """
    Make BIRD gold SQL friendlier to Postgres:
      - backticks → double quotes (lower-cased to match migrated schema)
      - CAST ... AS REAL → AS DOUBLE PRECISION
      - IFNULL(a,b) → COALESCE(a,b)
    """
    if not sql_text:
        return sql_text

    # 1) `Identifier` -> "identifier" (lowercase)
    def _bt_sub(m):
        return '"' + m.group(1).strip().lower() + '"'
    sql_text = BACKTICK_IDENT.sub(_bt_sub, sql_text)

    # 2) AS REAL → AS DOUBLE PRECISION (covers CAST/expressions)
    sql_text = re.sub(r"\bAS\s+REAL\b", "AS DOUBLE PRECISION", sql_text, flags=re.IGNORECASE)

    # 3) IFNULL(a,b) → COALESCE(a,b)
    #    naive but effective; doesn't try to balance nested parens deeply
    sql_text = re.sub(r"\bIFNULL\s*\(", "COALESCE(", sql_text, flags=re.IGNORECASE)

    return sql_text

def wrap_select_limit1(sql_text: str) -> str:
    """Cap results safely to 1 row via a wrapper CTE."""
    return f"WITH __q AS (\n{(sql_text or '').rstrip(';')}\n) SELECT * FROM __q LIMIT 1;"

def connect(dbname):
    return psycopg2.connect(
        dbname=dbname, user=PG_USER, password=PG_PASSWORD,
        host=PG_HOST, port=PG_PORT
    )

def try_exec(dbname: str, role: str, sql_wrapped: str):
    try:
        with connect(dbname) as conn, conn.cursor() as cur:
            conn.autocommit = True
            cur.execute("SET statement_timeout = '15s';")
            cur.execute("SET lock_timeout = '5s';")
            cur.execute("SET idle_in_transaction_session_timeout = '10s';")
            cur.execute("SET search_path TO public;")
            # switch role
            cur.execute(sql.SQL('SET ROLE {}').format(sql.Identifier(role)))

            try:
                cur.execute(sql_wrapped)
                # success → reset and return
                cur.execute("RESET ROLE;")
                return True, "", ""
            except Exception as e:
                # capture original error
                code = getattr(e, 'pgcode', '') or ''
                msg  = str(e).replace('\n', ' ')[:400]
                # rollback this failed tx, then reset role safely
                try:
                    conn.rollback()
                except Exception:
                    pass
                try:
                    cur.execute("RESET ROLE;")
                except Exception:
                    pass
                return False, code, msg
    except Exception as e:
        code = getattr(e, 'pgcode', '') or ''
        msg  = str(e).replace('\n', ' ')[:400]
        return False, code, msg


def main():
    if not os.path.isfile(PAIRS_CSV):
        raise SystemExit(f"❌ Missing {PAIRS_CSV}. Run extract-questions-SQLs-bird.py first (CSV output).")

    out_rows = []
    with open(PAIRS_CSV, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            split    = (row.get("split") or "").strip()
            qid      = (row.get("qid") or "").strip()
            question = (row.get("question") or "").strip()
            sql_text = (row.get("gold_sql") or "").strip()
            db_id    = (row.get("db_id") or "").strip()
            evidence = (row.get("evidence") or "").strip()

            dbname = normalize_dbname(db_id)
            if not dbname or not sql_text:
                continue

            # Only evaluate SELECTs; mark others
            if is_mutating(sql_text) or not is_select(sql_text):
                out_rows.append({
                    "split": split,
                    "db_id": db_id,
                    "qid": qid,
                    "dbname": dbname,
                    "role": "",
                    "permit": 0,
                    "sqlstate": "SKIP",
                    "error": "mutating_or_nonselect_sql",
                    "question": question,
                    "sql_original": sql_text,
                    "sql_wrapped": "",
                    "evidence": evidence,
                })
                continue

            # Normalize to PG (handle backticks, REAL, IFNULL, etc.)
            sql_text_norm = normalize_sql_for_postgres(sql_text)
            sql_wrapped = wrap_select_limit1(sql_text_norm)

            # Evaluate for all four roles
            for suf in ROLE_SUFFIXES:
                role = f"{dbname}_{suf}"
                permitted, code, msg = try_exec(dbname, role, sql_wrapped)
                out_rows.append({
                    "split": split,
                    "db_id": db_id,
                    "qid": qid,
                    "dbname": dbname,
                    "role": role,
                    "permit": 1 if permitted else 0,
                    "sqlstate": code,
                    "error": "" if permitted else msg,
                    "question": question,
                    "sql_original": sql_text,       # keep original for transparency
                    "sql_wrapped": sql_wrapped,     # wrapped, normalized SQL actually executed
                    "evidence": evidence,
                })

    # Write out (in current folder)
    with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=[
            "split","db_id","qid","dbname","role","permit","sqlstate","error",
            "question","sql_original","sql_wrapped","evidence"
        ])
        w.writeheader()
        w.writerows(out_rows)

    total = sum(1 for r in out_rows if r["role"])
    permits = sum(1 for r in out_rows if r["role"] and r["permit"] == 1)
    print(f"✅ Wrote {OUT_CSV}")
    print(f"ℹ️ Evaluated {total} (role, query) pairs; permitted={permits}, denied={total-permits}")

if __name__ == "__main__":
    main()

