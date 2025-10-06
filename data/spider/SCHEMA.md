# Spider-AC Schema

Authoritative schema and summary for the Spider Access-Control dataset shipped in this repo.

## Summary
- **Total records:** 19624
- **Unique databases (`db_id`):** 153
- **Users (roles):** User_1, User_2, User_3, User_4
- **Label distribution (normalized):** PERMIT=8095, DENY=11529
- **Avg question length (chars):** 67.3
- **Avg query length (chars):** 92.9

## File
- `spider-access-control.jsonl` — one JSON object per line.

## Record fields
Each line typically contains these keys:

| Field | Type | Description |
|---|---|---|
| `access_policy_sql` | string | Consolidated GRANT statements for the DB (multiple users). |
| `db_id` | string | Spider database identifier (schema name). |
| `ground_truth_label` | string | Outcome label. In this file it may contain answers or error strings. For evaluation, we normalize to PERMIT vs DENY (any `ERROR...` ⇒ DENY; otherwise ⇒ PERMIT). |
| `query` | string | Reference SQL for the question. |
| `question` | string | Natural-language question. |
| `schema_ddl` | string | Schema snapshot (CREATE TABLE ...). |
| `user` | string | Role identifier for access control (`User_1`..`User_4`). |

## Normalization rule (for evaluation)
- If `ground_truth_label` begins with `ERROR`, treat as **DENY**.
- Otherwise (including `[]`, `[[...]]`, or result arrays), treat as **PERMIT**.

## Examples
**Example 1:**

```json
{
  "db_id": "department_management",
  "user": "User_1",
  "query": "SELECT count(*) FROM head WHERE age  >  56",
  "question": "How many heads of the departments are older than 56 ?",
  "ground_truth_label": "[[5]]",
  "access_policy_sql": "GRANT USAGE ON SCHEMA department_management TO department_management_User_1;\nGRANT SELECT (department_id, name, creation, ranking, budget_in_billions, num_employees) ON department_management.department TO department_management_User_1;\nGRANT SELECT (head_id, name, born_state, age) ON department_management.head TO department_management_User_1;\nGRANT SELECT (department_id, head_id, temporary_acting) ON department_management.management TO department_management_User_1;\nGRANT USAGE ON SCHEMA department_management TO department_management_User_2;\nGRANT SELECT (department_id, name, creation, ranking, ...",
  "schema_ddl": "CREATE TABLE department (department_id bigint, name text, creation text, ranking integer, budget_in_billions double precision, num_employees double precision);\nCREATE TABLE head (head_id bigint, name text, born_state text, age double precision);\nCREATE TABLE management (department_id bigint, head_id bigint, temporary_acting text);"
}
```

**Example 2:**

```json
{
  "db_id": "department_management",
  "user": "User_2",
  "query": "SELECT count(*) FROM head WHERE age  >  56",
  "question": "How many heads of the departments are older than 56 ?",
  "ground_truth_label": "ERROR: permission denied for table head\n",
  "access_policy_sql": "GRANT USAGE ON SCHEMA department_management TO department_management_User_1;\nGRANT SELECT (department_id, name, creation, ranking, budget_in_billions, num_employees) ON department_management.department TO department_management_User_1;\nGRANT SELECT (head_id, name, born_state, age) ON department_management.head TO department_management_User_1;\nGRANT SELECT (department_id, head_id, temporary_acting) ON department_management.management TO department_management_User_1;\nGRANT USAGE ON SCHEMA department_management TO department_management_User_2;\nGRANT SELECT (department_id, name, creation, ranking, ...",
  "schema_ddl": "CREATE TABLE department (department_id bigint, name text, creation text, ranking integer, budget_in_billions double precision, num_employees double precision);\nCREATE TABLE head (head_id bigint, name text, born_state text, age double precision);\nCREATE TABLE management (department_id bigint, head_id bigint, temporary_acting text);"
}
```

**Example 3:**

```json
{
  "db_id": "department_management",
  "user": "User_3",
  "query": "SELECT count(*) FROM head WHERE age  >  56",
  "question": "How many heads of the departments are older than 56 ?",
  "ground_truth_label": "ERROR: permission denied for table head\n",
  "access_policy_sql": "GRANT USAGE ON SCHEMA department_management TO department_management_User_1;\nGRANT SELECT (department_id, name, creation, ranking, budget_in_billions, num_employees) ON department_management.department TO department_management_User_1;\nGRANT SELECT (head_id, name, born_state, age) ON department_management.head TO department_management_User_1;\nGRANT SELECT (department_id, head_id, temporary_acting) ON department_management.management TO department_management_User_1;\nGRANT USAGE ON SCHEMA department_management TO department_management_User_2;\nGRANT SELECT (department_id, name, creation, ranking, ...",
  "schema_ddl": "CREATE TABLE department (department_id bigint, name text, creation text, ranking integer, budget_in_billions double precision, num_employees double precision);\nCREATE TABLE head (head_id bigint, name text, born_state text, age double precision);\nCREATE TABLE management (department_id bigint, head_id bigint, temporary_acting text);"
}
```

