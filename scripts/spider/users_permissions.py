#!/usr/bin/env python3
"""
Create four per-database roles and grant:
  • User_1  – all tables, all columns
  • User_2  – ~50 % tables, all columns
  • User_3  – all tables, ~50 % columns in each
  • User_4  – ~50 % tables, ~50 % columns in those tables
Writes a CSV “user_permissions.csv” listing every object/column set granted.
"""

import psycopg2, csv, traceback, random

# ── Connection parameters ────────────────────────────────────────────────────
PG_ADMIN_DB = os.getenv("PG_ADMIN_DB", "postgres")
PG_USER     = os.getenv("PG_USER", "username")
PG_PASSWORD = os.getenv("PG_PASSWORD", "password")
PG_HOST     = os.getenv("PG_HOST", "localhost")
PG_PORT     = int(os.getenv("PG_PORT", 5432))

USER_LABELS = ["User_1", "User_2", "User_3", "User_4"]
CSV_OUTPUT  = "user_permissions.csv"

# ── Helpers ──────────────────────────────────────────────────────────────────
def connect(dbname):
    return psycopg2.connect(
        dbname=dbname, user=PG_USER, password=PG_PASSWORD,
        host=PG_HOST, port=PG_PORT
    )

def get_databases():
    with connect(PG_ADMIN_DB) as c, c.cursor() as cur:
        cur.execute("SELECT datname FROM pg_database WHERE datistemplate = false;")
        return sorted(r[0] for r in cur.fetchall())

def q(col):
    """quote if needed"""
    return f'"{col}"' if (not col.isidentifier() or "%" in col) else col

# ── Main logic ───────────────────────────────────────────────────────────────
def setup_permissions():
    rows = []

    for db in get_databases():
        print(f"\n🔍 Database: {db}")
        with connect(db) as conn, conn.cursor() as cur:
            conn.autocommit = True

            # ── gather schema info
            cur.execute("""
                SELECT table_name, column_name
                  FROM information_schema.columns
                 WHERE table_schema='public'
              ORDER BY table_name, ordinal_position;
            """)
            schema = {}
            for tbl, col in cur.fetchall():
                schema.setdefault(tbl, []).append(col)

            tables       = list(schema.keys())
            random.shuffle(tables)                    # random split each run
            half_tables  = tables[: len(tables)//2 ]

            # ── per-role grants
            for label in USER_LABELS:
                role = f'{db}_{label}'
                print(f"  • (re)creating role {role}")

                try:
                    cur.execute(f'DROP ROLE IF EXISTS "{role}";')
                    cur.execute(f'CREATE ROLE "{role}" LOGIN PASSWORD %s;', ('pass123',))
                    cur.execute(f'GRANT USAGE ON SCHEMA public TO "{role}";')
                    cur.execute(f'REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM "{role}";')
                except Exception as e:
                    print("    ❌ role create error →", e)
                    traceback.print_exc()
                    continue

                # ---- User_1 : everything --------------------------------------------------
                if label == "User_1":
                    cur.execute(f'GRANT SELECT ON ALL TABLES IN SCHEMA public TO "{role}";')
                    for tbl, cols in schema.items():
                        rows.append((db, role, tbl, ",".join(cols)))

                # ---- User_2 : half the tables, all their cols ----------------------------
                elif label == "User_2":
                    for tbl in half_tables:
                        cur.execute(f'GRANT SELECT ON "{tbl}" TO "{role}";')
                        rows.append((db, role, tbl, ",".join(schema[tbl])))

                # ---- User_3 : all tables, half the columns each --------------------------
                elif label == "User_3":
                    for tbl, cols in schema.items():
                        allowed = cols[: len(cols)//2 ]
                        if not allowed: continue
                        col_list = ", ".join(q(c) for c in allowed)
                        cur.execute(f'GRANT SELECT ({col_list}) ON "{tbl}" TO "{role}";')
                        rows.append((db, role, tbl, ",".join(allowed)))

                # ---- User_4 : half the tables, half the columns --------------------------
                else:  # User_4
                    for tbl in half_tables:
                        cols = schema[tbl]
                        allowed = cols[: len(cols)//2 ]
                        if not allowed: continue
                        col_list = ", ".join(q(c) for c in allowed)
                        cur.execute(f'GRANT SELECT ({col_list}) ON "{tbl}" TO "{role}";')
                        rows.append((db, role, tbl, ",".join(allowed)))

    # ── tidy CSV output grouped by DB then User_1-4 order
    def sort_key(r):
        db, role, *_ = r
        idx = next(i for i, lbl in enumerate(USER_LABELS) if role.endswith(lbl))
        return (db, idx)

    rows.sort(key=sort_key)

    with open(CSV_OUTPUT, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["database", "user", "object", "accessible_columns"])
        w.writerows(rows)

    print(f"\n✅ Finished. Grants written to {CSV_OUTPUT}")

# ── Run ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    setup_permissions()
