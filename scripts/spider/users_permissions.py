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
PG_ADMIN_DB = "postgres"
PG_USER     = "dorde"
PG_PASSWORD = "project123"
PG_HOST     = "localhost"
PG_PORT     = 5432

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



# #!/usr/bin/env python3
# import psycopg2
# import csv
# import traceback

# # ——— Configuration ———
# PG_ADMIN_DB = "postgres"
# PG_USER     = "dorde"
# PG_PASSWORD = "project123"
# PG_HOST     = "localhost"
# PG_PORT     = 5432

# # Four per-DB user roles
# USER_LABELS = ["User_1", "User_2", "User_3", "User_4"]
# CSV_OUTPUT  = "user_permissions.csv"

# def connect(dbname):
#     return psycopg2.connect(
#         dbname=dbname,
#         user=PG_USER,
#         password=PG_PASSWORD,
#         host=PG_HOST,
#         port=PG_PORT
#     )

# def get_databases():
#     with connect(PG_ADMIN_DB) as c, c.cursor() as cur:
#         cur.execute("SELECT datname FROM pg_database WHERE datistemplate = false;")
#         return sorted(r[0] for r in cur.fetchall())

# def safe_col(col):
#     # quote only if needed (e.g. contains special chars)
#     return f'"{col}"' if not col.isidentifier() or "%" in col else col

# def setup_permissions():
#     all_rows = []

#     for db in get_databases():
#         print(f"\n🔍 Database: {db}")
#         with connect(db) as conn, conn.cursor() as cur:
#             conn.autocommit = True

#             # build table → [columns] map
#             cur.execute("""
#                 SELECT table_name, column_name
#                   FROM information_schema.columns
#                  WHERE table_schema='public'
#               ORDER BY table_name, ordinal_position;
#             """)
#             schema = {}
#             for tbl, col in cur.fetchall():
#                 schema.setdefault(tbl, []).append(col)

#             tables      = list(schema.keys())
#             half_tables = tables[: len(tables)//2 ]

#             # create & assign each of the four users
#             for label in USER_LABELS:
#                 user = f"{db}_{label}"
#                 print(f" • Creating / preparing: {user}")

#                 # 1) drop & recreate role, clear any old table grants
#                 try:
#                     cur.execute(f'DROP ROLE IF EXISTS "{user}";')
#                     cur.execute(f'CREATE ROLE "{user}" LOGIN PASSWORD \'pass123\';')
#                     cur.execute(f'REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM "{user}";')
#                 except Exception as e:
#                     print(f"   ❌ Could not CREATE ROLE {user}: {e}")
#                     traceback.print_exc()
#                     continue

#                 # 2) assign the right grants
#                 try:
#                     if label == "User_1":
#                         # full table+column access
#                         cur.execute(f'GRANT SELECT ON ALL TABLES IN SCHEMA public TO "{user}";')
#                         for tbl, cols in schema.items():
#                             all_rows.append((db, user, tbl, ",".join(cols)))

#                     elif label == "User_2":
#                         # half the tables, all their columns
#                         cur.execute(f'GRANT SELECT ON ALL TABLES IN SCHEMA public TO "{user}";')
#                         # revoke the other half
#                         for tbl in tables:
#                             if tbl not in half_tables:
#                                 cur.execute(f'REVOKE SELECT ON "{tbl}" FROM "{user}";')
#                         for tbl in half_tables:
#                             all_rows.append((db, user, tbl, ",".join(schema[tbl])))

#                     elif label == "User_3":
#                         # all tables, but only half the columns each
#                         for tbl, cols in schema.items():
#                             half = cols[: len(cols)//2 ]
#                             if not half:
#                                 continue
#                             # clean slate: revoke any full-table access
#                             cur.execute(f'REVOKE SELECT ON "{tbl}" FROM "{user}";')
#                             # then grant just those columns
#                             col_list = ", ".join(safe_col(c) for c in half)
#                             cur.execute(
#                                 f'GRANT SELECT ({col_list}) ON "{tbl}" TO "{user}";'
#                             )
#                             all_rows.append((db, user, tbl, ",".join(half)))

#                     else:  # User_4
#                         # half the tables, and within each of those half-tables only half the columns
#                         for tbl in half_tables:
#                             cols = schema[tbl]
#                             half = cols[: len(cols)//2 ]
#                             if not half:
#                                 continue
#                             cur.execute(f'REVOKE SELECT ON "{tbl}" FROM "{user}";')
#                             col_list = ", ".join(safe_col(c) for c in half)
#                             cur.execute(
#                                 f'GRANT SELECT ({col_list}) ON "{tbl}" TO "{user}";'
#                             )
#                             all_rows.append((db, user, tbl, ",".join(half)))

#                 except Exception as e:
#                     print(f"   ⚠️ Error assigning perms for {user}: {e}")
#                     traceback.print_exc()

#     # sort so each DB’s four users appear together
#     def sort_key(r):
#         database, user, *_ = r
#         for idx, lbl in enumerate(USER_LABELS):
#             if user.endswith(lbl):
#                 return (database, idx)
#         return (database, len(USER_LABELS))

#     all_rows.sort(key=sort_key)

#     # write out CSV
#     with open(CSV_OUTPUT, "w", newline="") as f:
#         w = csv.writer(f)
#         w.writerow(["database", "user", "object", "accessible_columns"])
#         w.writerows(all_rows)

#     print(f"\n✅ Permissions assigned. CSV written: {CSV_OUTPUT}")

# if __name__ == "__main__":
#     setup_permissions()
