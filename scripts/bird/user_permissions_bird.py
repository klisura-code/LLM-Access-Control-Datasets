#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Create four per-database roles and grant:
  • User_1  – all tables, all columns
  • User_2  – ~50% tables, all columns
  • User_3  – all tables, ~50% columns in each
  • User_4  – ~50% tables, ~50% columns in those tables

Targets ALL user databases (excludes system DBs and 'birddb' seed).
Writes: user_permissions_bird.csv
"""

import psycopg2, csv, traceback, random, os

# ── Connection parameters (BIRD stack) ───────────────────────────────────────
PG_ADMIN_DB = os.getenv("PG_ADMIN_DB", "postgres")  # control/database-listing DB
PG_USER     = os.getenv("PG_USER", "username")
PG_PASSWORD = os.getenv("PG_PASSWORD", "password")
PG_HOST     = os.getenv("PG_HOST", "localhost")
PG_PORT     = int(os.getenv("PG_PORT", 5433))

USER_LABELS = ["User_1", "User_2", "User_3", "User_4"]
CSV_OUTPUT  = "user_permissions_bird.csv"

# Reproducible halves (set env SEED to override)
SEED = int(os.getenv("SEED", "1337"))
random.seed(SEED)

SYSTEM_DB_EXCLUDES = {"postgres", "template0", "template1", PG_ADMIN_DB}

def connect(dbname):
    return psycopg2.connect(
        dbname=dbname, user=PG_USER, password=PG_PASSWORD,
        host=PG_HOST, port=PG_PORT
    )

def get_databases():
    """
    Return candidate DBs: exclude system DBs and any without user tables in 'public'.
    """
    with connect(PG_ADMIN_DB) as c, c.cursor() as cur:
        cur.execute("""
            SELECT d.datname
              FROM pg_database d
             WHERE d.datistemplate = false
               AND d.datname NOT IN %s
             ORDER BY d.datname;
        """, (tuple(SYSTEM_DB_EXCLUDES),))
        all_dbs = [r[0] for r in cur.fetchall()]

    # Keep only DBs that actually have tables in schema 'public'
    keep = []
    for db in all_dbs:
        try:
            with connect(db) as c, c.cursor() as cur:
                cur.execute("""
                    SELECT COUNT(*)
                      FROM information_schema.tables
                     WHERE table_schema = 'public'
                """)
                cnt = cur.fetchone()[0]
                if cnt and cnt > 0:
                    keep.append(db)
        except Exception:
            # If we can't access it, skip silently
            continue
    return keep

def q_ident(col: str) -> str:
    return '"' + str(col).replace('"', '""') + '"'

def setup_permissions():
    rows = []

    targets = get_databases()
    print(f"🔎 Databases to configure ({len(targets)}): {targets}")

    for db in targets:
        print(f"\n🗂️  Database: {db}")
        with connect(db) as conn, conn.cursor() as cur:
            conn.autocommit = True

            # gather schema info
            cur.execute("""
                SELECT table_name, column_name
                  FROM information_schema.columns
                 WHERE table_schema = 'public'
              ORDER BY table_name, ordinal_position;
            """)
            schema = {}
            for tbl, col in cur.fetchall():
                schema.setdefault(tbl, []).append(col)

            if not schema:
                print("  ⚠️  No tables in public schema, skipping.")
                continue

            tables = sorted(schema.keys())
            rnd_tables = tables[:]
            random.shuffle(rnd_tables)
            half_tables = set(rnd_tables[: max(1, len(rnd_tables)//2)])

            # ensure base usage and clean slate for each role
            for label in USER_LABELS:
                role = f'{db}_{label}'
                print(f"  • (re)creating role {role}")
                try:
                    cur.execute(f'DROP ROLE IF EXISTS {q_ident(role)};')
                    cur.execute(f'CREATE ROLE {q_ident(role)} LOGIN PASSWORD %s;', ('pass123',))
                    # Base privileges and cleanup
                    cur.execute(f'GRANT USAGE ON SCHEMA public TO {q_ident(role)};')
                    cur.execute(f'REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM {q_ident(role)};')
                    cur.execute(f'REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public FROM {q_ident(role)};')
                    # Make future tables default to no access for these roles (explicit grants only)
                    cur.execute("ALTER DEFAULT PRIVILEGES IN SCHEMA public REVOKE ALL ON TABLES FROM PUBLIC;")
                except Exception as e:
                    print("    ❌ role create error →", e)
                    traceback.print_exc()
                    continue

            # User_1: everything (all tables, all columns)
            label = "User_1"
            role = f'{db}_{label}'
            cur.execute(f'GRANT SELECT ON ALL TABLES IN SCHEMA public TO {q_ident(role)};')
            for tbl, cols in schema.items():
                rows.append((db, role, tbl, ",".join(cols)))

            # User_2: half the tables, all their columns
            label = "User_2"
            role = f'{db}_{label}'
            for tbl in tables:
                if tbl in half_tables:
                    cur.execute(f'GRANT SELECT ON {q_ident(tbl)} TO {q_ident(role)};')
                    rows.append((db, role, tbl, ",".join(schema[tbl])))

            # User_3: all tables, half the columns each
            label = "User_3"
            role = f'{db}_{label}'
            for tbl, cols in schema.items():
                if not cols:
                    continue
                allowed = cols[: max(1, len(cols)//2)]
                col_list = ", ".join(q_ident(c) for c in allowed)
                cur.execute(f'GRANT SELECT ({col_list}) ON {q_ident(tbl)} TO {q_ident(role)};')
                rows.append((db, role, tbl, ",".join(allowed)))

            # User_4: half the tables, half the columns
            label = "User_4"
            role = f'{db}_{label}'
            for tbl in tables:
                if tbl not in half_tables:
                    continue
                cols = schema[tbl]
                if not cols:
                    continue
                allowed = cols[: max(1, len(cols)//2)]
                col_list = ", ".join(q_ident(c) for c in allowed)
                cur.execute(f'GRANT SELECT ({col_list}) ON {q_ident(tbl)} TO {q_ident(role)};')
                rows.append((db, role, tbl, ",".join(allowed)))

    # sort output grouped by db, then User_1..User_4
    def sort_key(r):
        db, role, *_ = r
        idx = next(i for i, lbl in enumerate(USER_LABELS) if role.endswith(lbl))
        return (db, idx)

    rows.sort(key=sort_key)

    with open(CSV_OUTPUT, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["database", "user", "object", "accessible_columns"])
        w.writerows(rows)

    print(f"\n✅ Finished. Grants applied. CSV written → {CSV_OUTPUT}")

if __name__ == "__main__":
    setup_permissions()

