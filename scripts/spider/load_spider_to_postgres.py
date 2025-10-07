import os
import sqlite3
import psycopg2
from psycopg2 import sql
import datetime
import subprocess

# PostgreSQL connection settings
PG_USER = os.getenv("PG_USER", "username")
PG_PASSWORD = os.getenv("PG_PASSWORD", "password")
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", 5432))

# Path to Spider database folder (anonymized for submission)
SPIDER_DB_PATH = os.getenv(
    "SPIDER_DB_PATH",
    os.path.expanduser("~/path/to/spider/database")
)

def map_sqlite_type_to_postgres(sqlite_type, col_name=None):
    sqlite_type = (sqlite_type or "").upper()
    col_name = col_name.lower() if col_name else ""

    if any(kw in col_name for kw in ['zip', 'event', 'type', 'status', 'category']):
        return "TEXT"
    if "INT" in sqlite_type:
        if any(kw in col_name for kw in ["id", "phone", "number", "count", "code"]):
            return "BIGINT"
        return "INTEGER"
    elif any(word in sqlite_type for word in ["CHAR", "CLOB", "TEXT", "VARCHAR"]):
        return "TEXT"
    elif "BLOB" in sqlite_type:
        return "BYTEA"
    elif any(word in sqlite_type for word in ["REAL", "FLOA", "DOUB", "NUMERIC"]):
        return "DOUBLE PRECISION"
    elif "DATE" in sqlite_type or "TIME" in sqlite_type:
        return "TIMESTAMP"
    return "TEXT"

def create_postgres_database(db_name):
    try:
        subprocess.run([
            "createdb",
            "-h", PG_HOST,
            "-p", str(PG_PORT),
            "-U", PG_USER,
            db_name
        ], check=True)
        print(f"✅ Created PostgreSQL database: {db_name}")
    except subprocess.CalledProcessError:
        print(f"⚠️  Database {db_name} might already exist. Continuing.")

def migrate_sqlite_to_postgres(sqlite_path, db_name):
    print(f"→ Migrating: {db_name}")

    sqlite_conn = sqlite3.connect(sqlite_path)
    sqlite_conn.text_factory = bytes
    sqlite_cursor = sqlite_conn.cursor()

    pg_conn = psycopg2.connect(
        dbname=db_name,
        user=PG_USER,
        password=PG_PASSWORD,
        host=PG_HOST,
        port=PG_PORT
    )
    pg_cursor = pg_conn.cursor()

    sqlite_cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [row[0].decode('utf-8', errors='replace') if isinstance(row[0], bytes) else row[0]
              for row in sqlite_cursor.fetchall()]

    for table in tables:
        table = table.lower()
        sqlite_cursor.execute(f'PRAGMA table_info("{table}")')
        columns_info = sqlite_cursor.fetchall()

        columns = []
        col_defs_list = []

        for col in columns_info:
            col_name = col[1]
            col_type = col[2]
            if isinstance(col_name, bytes):
                col_name = col_name.decode('utf-8', errors='replace')
            if isinstance(col_type, bytes):
                col_type = col_type.decode('utf-8', errors='replace')

            col_name = col_name.lower()
            pg_type = map_sqlite_type_to_postgres(col_type, col_name)
            columns.append(col_name)
            col_defs_list.append(f'"{col_name}" {pg_type}')  # ← quoted here

        col_defs = ", ".join(col_defs_list)

        pg_cursor.execute(sql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(
            sql.Identifier(table)
        ))
        pg_cursor.execute(sql.SQL("CREATE TABLE {} ({})").format(
            sql.Identifier(table), sql.SQL(col_defs)
        ))

        sqlite_cursor.execute(f'SELECT * FROM "{table}"')
        rows = sqlite_cursor.fetchall()

        if rows:
            placeholders = ", ".join(["%s"] * len(columns))
            insert_query = sql.SQL("INSERT INTO {} VALUES (" + placeholders + ")").format(
                sql.Identifier(table)
            )

            for row in rows:
                clean_row = []
                for idx, val in enumerate(row):
                    if isinstance(val, bytes):
                        try:
                            val = val.decode('utf-8', errors='replace')
                        except:
                            val = ''

                    if isinstance(val, str):
                        if val.strip() == "0000-00-00":
                            val = None
                        elif val.strip().upper() == 'NULL':
                            val = None
                        elif val.strip().upper() == 'T' and 'precipitation' in col_defs_list[idx].lower():
                            val = 0.0

                    target_pg_type = col_defs_list[idx].split()[-1].upper()
                    if val == '' and target_pg_type not in ['TEXT', 'VARCHAR']:
                        val = None

                    if target_pg_type in ['TIMESTAMP', 'DATE'] and isinstance(val, int):
                        try:
                            year = val // 10000
                            month = (val % 10000) // 100
                            day = val % 100
                            val = datetime.date(year, month, day)
                        except Exception:
                            val = None

                    clean_row.append(val)

                pg_cursor.execute(insert_query, clean_row)

    pg_conn.commit()
    sqlite_conn.close()
    pg_conn.close()
    print(f"✅ Done: {db_name}\n")

def main():
    os.environ["PGPASSWORD"] = PG_PASSWORD

    for db_folder in os.listdir(SPIDER_DB_PATH):
        db_dir = os.path.join(SPIDER_DB_PATH, db_folder)

        db_file = os.path.join(db_dir, "database.sqlite")
        if not os.path.isfile(db_file):
            db_file = os.path.join(db_dir, f"{db_folder}.sqlite")
        if not os.path.isfile(db_file):
            print(f"⚠️  Skipping {db_folder} — no .sqlite file found.")
            continue

        db_name = db_folder.lower()
        create_postgres_database(db_name)
        migrate_sqlite_to_postgres(db_file, db_name)

    print("🎉 All Spider databases migrated into individual PostgreSQL databases (lowercase + quoted)!")

if __name__ == "__main__":
    main()
