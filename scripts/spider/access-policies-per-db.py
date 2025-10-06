import pandas as pd
from collections import defaultdict

# Load user_permissions.csv (generated from your grant script)
perms = pd.read_csv("user_permissions.csv")

# Helper to quote identifiers if needed
def q(identifier: str) -> str:
    return f'"{identifier}"' if not identifier.isidentifier() else identifier

# Step 1: Collect GRANT statements per (database, user, table)
grant_map = defaultdict(list)

for _, row in perms.iterrows():
    db = row["database"]
    user = row["user"]
    table = row["object"]
    columns = row["accessible_columns"]

    # GRANT USAGE ON SCHEMA - once per user per database
    usage_stmt = f'GRANT USAGE ON SCHEMA {q(db)} TO {q(user)};'
    if usage_stmt not in grant_map[db]:
        grant_map[db].append(usage_stmt)

    # GRANT SELECT (col1, col2, ...) ON table TO user
    if pd.notna(columns) and columns.strip():
        col_list = ", ".join(q(col.strip()) for col in columns.split(","))
        select_stmt = f'GRANT SELECT ({col_list}) ON {q(db)}.{q(table)} TO {q(user)};'
        grant_map[db].append(select_stmt)

# Step 2: Format as DataFrame
rows = [{"db_id": db, "access_policy_sql": "\n".join(stmts)} for db, stmts in grant_map.items()]
df_out = pd.DataFrame(rows)

# Step 3: Save to CSV
df_out.to_csv("db_access_policies.csv", index=False)
print("✅ Saved: db_access_policies.csv")
