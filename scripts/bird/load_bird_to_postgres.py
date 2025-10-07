#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, re, sqlite3, datetime, subprocess, psycopg2
from psycopg2 import sql, extras
from concurrent.futures import ProcessPoolExecutor, as_completed

# ── Postgres connection ───────────────────────────────────────────────────────
PG_USER = os.getenv("PG_USER", "username")
PG_PASSWORD = os.getenv("PG_PASSWORD", "password")
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", 5433))  # Postgres exposed on 5433

# ── Path to all SQLite DBs ───────────────────────────────────────────────────
BIRD_DB_ROOT = os.getenv(
    "BIRD_DB_ROOT",
    os.path.expanduser("~/path/to/BIRD/databases")
)

# ── Performance tuning ───────────────────────────────────────────────────────
BATCH_SIZE = 5000
MAX_WORKERS = 4


# ── Helpers ──────────────────────────────────────────────────────────────────
def map_sqlite_type_to_postgres(sqlite_type, col_name=None):
    sqlite_type = (sqlite_type or "").upper()
    col_name = (col_name or "").lower()

    if any(kw in col_name for kw in ["zip", "event", "type", "status", "category"]):
        return "TEXT"

    # Always prefer BIGINT over INTEGER to prevent overflows
    if "INT" in sqlite_type:
        return "BIGINT"
    if any(word in sqlite_type for word in ["CHAR", "CLOB", "TEXT", "VARCHAR"]):
        return "TEXT"
    if "BLOB" in sqlite_type:
        return "BYTEA"
    if any(word in sqlite_type for word in ["REAL", "FLOA", "DOUB", "NUMERIC"]):
        return "DOUBLE PRECISION"
    if "DATE" in sqlite_type or "TIME" in sqlite_type:
        return "TIMESTAMP"
    return "TEXT"


def createdb(db_name: str):
    try:
        subprocess.run(
            ["createdb", "-h", PG_HOST, "-p", str(PG_PORT), "-U", PG_USER, db_name],
            check=True,
            env={**os.environ, "PGPASSWORD": PG_PASSWORD},
        )
        print(f"✅ created: {db_name}")
    except subprocess.CalledProcessError:
        print(f"⚠️  exists:  {db_name} (continuing)")


def connect_pg(db_name: str):
    return psycopg2.connect(
        dbname=db_name, user=PG_USER, password=PG_PASSWORD, host=PG_HOST, port=PG_PORT
    )


def copy_table(cur, table_l, rows_iter, ncols):
    template = "(" + ",".join(["%s"] * ncols) + ")"
    batch = []
    for row in rows_iter:
        batch.append(list(row))
        if len(batch) >= BATCH_SIZE:
            extras.execute_values(
                cur,
                sql.SQL("INSERT INTO {} VALUES %s").format(sql.Identifier(table_l)),
                batch,
                template=template,
            )
            batch.clear()
    if batch:
        extras.execute_values(
            cur,
            sql.SQL("INSERT INTO {} VALUES %s").format(sql.Identifier(table_l)),
            batch,
            template=template,
        )


def is_header_like(row_vals, columns):
    hits, total = 0, 0
    norm = lambda s: re.sub(r"[\s_]+", "", str(s).strip().lower())
    col_norms = [norm(c) for c in columns]
    for i, v in enumerate(row_vals):
        if v is None:
            continue
        total += 1
        if norm(v) == col_norms[i]:
            hits += 1
    return total > 0 and (hits / total) >= 0.6


def parse_dateish(val):
    if val is None:
        return None
    if isinstance(val, int):
        if 1 <= val <= 9999:
            try:
                return datetime.date(int(val), 1, 1)
            except Exception:
                return None
        return None
    s = str(val).strip()
    if not s or s.upper() in {"NULL", "N/A", "NA"}:
        return None
    if re.fullmatch(r"\d{4}", s):
        try:
            return datetime.date(int(s), 1, 1)
        except Exception:
            return None
    m = re.fullmatch(r"(\d{4})[-/](\d{1,2})[-/](\d{1,2})", s)
    if m:
        y, mo, d = map(int, m.groups())
        try:
            return datetime.date(y, mo, d)
        except Exception:
            return None
    return None


def migrate_one_sqlite(sqlite_path: str):
    dbid = os.path.splitext(os.path.basename(sqlite_path))[0]
    pg_db = dbid.lower()  # ← no prefix
    createdb(pg_db)

    sqlite_conn = sqlite3.connect(sqlite_path)
    sqlite_conn.text_factory = bytes
    s_cur = sqlite_conn.cursor()

    pg_conn = connect_pg(pg_db)
    pg_conn.autocommit = False
    p_cur = pg_conn.cursor()

    p_cur.execute("SET synchronous_commit TO OFF;")
    p_cur.execute("SET client_min_messages TO WARNING;")
    p_cur.execute("SET work_mem TO '128MB';")
    p_cur.execute("SET maintenance_work_mem TO '256MB';")

    s_cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [
        (row[0].decode("utf-8", "replace") if isinstance(row[0], bytes) else row[0])
        for row in s_cur.fetchall()
    ]

    for table in tables:
        table_l = table.lower()

        s_cur.execute(f'PRAGMA table_info("{table}")')
        cols_info = s_cur.fetchall()

        columns, col_defs_list = [], []
        for col in cols_info:
            col_name = col[1]
            col_type = col[2]
            if isinstance(col_name, bytes):
                col_name = col_name.decode("utf-8", "replace")
            if isinstance(col_type, bytes):
                col_type = col_type.decode("utf-8", "replace")
            col_name = col_name.lower()
            pg_type = map_sqlite_type_to_postgres(col_type, col_name)
            columns.append(col_name)
            col_defs_list.append(f'"{col_name}" {pg_type}')
        col_defs = ", ".join(col_defs_list)

        p_cur.execute(sql.SQL('DROP TABLE IF EXISTS {} CASCADE').format(sql.Identifier(table_l)))
        p_cur.execute(sql.SQL('CREATE UNLOGGED TABLE {} ({})').format(sql.Identifier(table_l), sql.SQL(col_defs)))

        s_cur.execute(f'SELECT * FROM "{table}"')

        def gen_rows():
            first_row_checked = False
            for row in s_cur:
                raw = list(row)

                if not first_row_checked:
                    header_check_vals = []
                    for idx, val in enumerate(raw):
                        if isinstance(val, bytes):
                            try:
                                val = val.decode("utf-8", "replace")
                            except Exception:
                                val = ""
                        header_check_vals.append(val)
                    if is_header_like(header_check_vals, columns):
                        first_row_checked = True
                        continue
                    first_row_checked = True

                clean = []
                for idx, val in enumerate(raw):
                    if isinstance(val, bytes):
                        try:
                            val = val.decode("utf-8", "replace")
                        except Exception:
                            val = ""
                    if isinstance(val, str):
                        v = val.strip()
                        if v in {"0000-00-00", "0000/00/00"} or v.upper() == "NULL":
                            val = None
                    target_pg_type = col_defs_list[idx].split()[-1].upper()
                    if val == "" and target_pg_type not in ["TEXT", "VARCHAR"]:
                        val = None
                    if target_pg_type in ["TIMESTAMP", "DATE"]:
                        if isinstance(val, int):
                            if 10101 <= val <= 99991231:
                                try:
                                    y = val // 10000
                                    m = (val % 10000) // 100
                                    d = val % 100
                                    val = datetime.date(y, m, d)
                                except Exception:
                                    val = parse_dateish(val)
                            else:
                                val = parse_dateish(val)
                        elif isinstance(val, str):
                            val = parse_dateish(val)
                    clean.append(val)

                if all(c is None for c in clean):
                    continue
                yield clean

        copy_table(p_cur, table_l, gen_rows(), len(columns))
        pg_conn.commit()
        p_cur.execute(sql.SQL('ALTER TABLE {} SET LOGGED').format(sql.Identifier(table_l)))
        pg_conn.commit()

    p_cur.close()
    pg_conn.close()
    sqlite_conn.close()
    print(f"✅ done: {pg_db}")
    return pg_db


def find_all_sqlites(root_dir: str):
    hits = []
    for r, _, files in os.walk(root_dir):
        for f in files:
            if f.lower().endswith((".db", ".sqlite", ".sqlite3")):
                hits.append(os.path.join(r, f))
    return sorted(hits)


def get_existing_dbs():
    cmd = [
        "psql", "-h", PG_HOST, "-p", str(PG_PORT),
        "-U", PG_USER, "-d", "birddb",
        "-Atc", "SELECT datname FROM pg_database WHERE datistemplate = false;"
    ]
    result = subprocess.run(
        cmd, check=True, capture_output=True, text=True,
        env={**os.environ, "PGPASSWORD": PG_PASSWORD}
    )
    return set(line.strip() for line in result.stdout.splitlines() if line.strip())


def main():
    os.environ["PGPASSWORD"] = PG_PASSWORD

    sqlites = find_all_sqlites(BIRD_DB_ROOT)
    print(f"📦 Found {len(sqlites)} SQLite file(s) under {BIRD_DB_ROOT}")

    if not sqlites:
        print("❌ No SQLite files found.")
        return

    existing = get_existing_dbs()
    todo = []
    for sp in sqlites:
        dbid = os.path.splitext(os.path.basename(sp))[0]
        pg_db = dbid.lower()
        if pg_db in existing:
            print(f"⏭️  skipping {pg_db} (already exists)")
            continue
        todo.append(sp)

    print(f"➡️  Will migrate {len(todo)} database(s).")

    if todo:
        with ProcessPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futs = [ex.submit(migrate_one_sqlite, sp) for sp in todo]
            for f in as_completed(futs):
                try:
                    f.result()
                except Exception as e:
                    print("❌ worker failed:", e)

    print("🎉 Full BIRD databases migration completed.")


if __name__ == "__main__":
    main()

