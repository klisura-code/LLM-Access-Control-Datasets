#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Read user_permissions_bird.csv and produce:
  • db_access_policies.csv  – per-DB consolidated GRANT statements for all roles
  • db_access_policies_full.csv – adds a schema DDL snapshot (CREATE TABLE ...)

Assumes roles already exist (from user_permissions_bird.py). This script only GENERATES SQL text.
"""

import csv, os, psycopg2

# ── Connection params ────────────────────────────────────────────────────────
PG_USER = os.getenv("PG_USER", "username")
PG_PASSWORD = os.getenv("PG_PASSWORD", "password")
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", 5433))
PG_ADMIN_DB = os.getenv("PG_ADMIN_DB", "postgres") # <- use the built-in control DB

INPUT_CSV  = "user_permissions_bird.csv"   # <- your generated file
OUT_POL    = "db_access_policies.csv"
OUT_FULL   = "db_access_policies_full.csv"

def connect(dbname):
    return psycopg2.connect(
        dbname=dbname, user=PG_USER, password=PG_PASSWORD,
        host=PG_HOST, port=PG_PORT
    )

def snapshot_schema_ddl(db):
    """Return a simple CREATE TABLE … snapshot for schema 'public'."""
    try:
        with connect(db) as conn, conn.cursor() as cur:
            cur.execute("""
                SELECT table_name, column_name, data_type
                  FROM information_schema.columns
                 WHERE table_schema='public'
              ORDER BY table_name, ordinal_position;
            """)
            rows = cur.fetchall()

        tables = {}
        for tbl, col, dtype in rows:
            tables.setdefault(tbl, []).append((col, dtype))

        ddls = []
        for tbl, cols in tables.items():
            col_defs = ", ".join(f'"{col}" {dtype}' for col, dtype in cols)
            ddls.append(f'CREATE TABLE "{tbl}" ({col_defs});')
        return "\n".join(ddls)
    except Exception as e:
        return f"-- ERROR generating schema for {db}: {e}"

def get_table_widths(db):
    """Return {table_name: column_count} for schema 'public'."""
    widths = {}
    with connect(db) as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT table_name, COUNT(*)
              FROM information_schema.columns
             WHERE table_schema='public'
          GROUP BY table_name;
        """)
        for tbl, cnt in cur.fetchall():
            widths[tbl] = int(cnt)
    return widths

def main():
    if not os.path.isfile(INPUT_CSV):
        raise SystemExit(f"❌ Missing {INPUT_CSV}. Run user_permissions_bird.py first.")

    # group rows by (db, role, table)
    by_db = {}
    with open(INPUT_CSV, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            db   = row["database"]
            role = row["user"]
            tbl  = row["object"]
            cols = row["accessible_columns"]
            by_db.setdefault(db, {}).setdefault(role, {}).setdefault(tbl, set()).update(
                c for c in cols.split(",") if c
            )

    pol_rows = []
    for db, roles in by_db.items():
        # cache actual widths once per DB
        try:
            widths = get_table_widths(db)
        except Exception:
            widths = {}

        policy_sqls = []
        for role, tables in roles.items():
            # ensure usage on schema
            policy_sqls.append(f'GRANT USAGE ON SCHEMA public TO "{role}";')

            for tbl, cols in tables.items():
                col_list_sorted = sorted(list(cols))
                width = widths.get(tbl)

                # if we know the width AND our set size >= width, treat as full-table grant
                if width is not None and len(col_list_sorted) >= width:
                    policy_sqls.append(f'GRANT SELECT ON "{tbl}" TO "{role}";')
                else:
                    col_idents = ", ".join(f'"{c}"' for c in col_list_sorted)
                    policy_sqls.append(f'GRANT SELECT ({col_idents}) ON "{tbl}" TO "{role}";')

        pol_rows.append({"db_id": db, "access_policy_sql": "\n".join(policy_sqls)})

    # Write db_access_policies.csv
    with open(OUT_POL, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["db_id", "access_policy_sql"])
        w.writeheader()
        for row in pol_rows:
            w.writerow(row)

    # Add schema snapshot → db_access_policies_full.csv
    with open(OUT_FULL, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["db_id", "access_policy_sql", "db_schema_ddl"])
        w.writeheader()
        for row in pol_rows:
            ddl = snapshot_schema_ddl(row["db_id"])
            w.writerow({
                "db_id": row["db_id"],
                "access_policy_sql": row["access_policy_sql"],
                "db_schema_ddl": ddl
            })

    print(f"✅ Wrote {OUT_POL} and {OUT_FULL}")

if __name__ == "__main__":
    main()

