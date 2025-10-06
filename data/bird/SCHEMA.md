# BIRD-AC Schema

This document describes the schema and key statistics for the **BIRD Access-Control (BIRD-AC)** dataset in this repository.

## Summary

- **Total examples:** 35,856 (after filtering)
- **Unique databases (`db_id`):** 80
- **Users (roles):** User_1, User_2, User_3, User_4
- **Label distribution:** PERMIT – 11,953 | DENY (`42501` insufficient privilege) – 23,903

## Files

- `bird-access-control.jsonl` — Final dataset (one JSON object per line).

## Record Format

| Field | Type | Description |
|---|---|---|
| `split` | string | Dataset split (e.g., dev or train). |
| `db_id` | string | Database identifier. |
| `user` | string | Role used for evaluation (User_1–User_4). |
| `qid` | string | Original BIRD question ID. |
| `question` | string | Natural-language question. |
| `sql` | string | Original SQL query. |
| `sql_wrapped` | string | Safe execution wrapper. |
| `decision` | string | PERMIT or DENY. |
| `sqlstate` | string | SQLSTATE code; empty or `42501`. |
| `policy_sql` | string | GRANT statements defining the policy. |
| `schema_ddl` | string | Database schema snapshot. |

## Example

```json
{
  "db_id": "california_schools",
  "user": "california_schools_User_2",
  "question": "What is the highest eligible free rate for K-12 students in Alameda County?",
  "sql": "SELECT `Free Meal Count (K-12)` / `Enrollment (K-12)` FROM frpm WHERE `County Name` = 'Alameda' ...",
  "decision": "DENY",
  "sqlstate": "42501",
  "error": "permission denied for table frpm",
  "policy_sql": "GRANT SELECT ON frpm TO california_schools_User_1; ...",
  "schema_ddl": "CREATE TABLE frpm (...);"
}
```

## Notes

- Each database has four roles (User_1–User_4) with progressively restricted access.
- Only PERMIT and `42501` DENY entries are included.
- Designed for evaluating **LLM access-control reasoning** and **policy-conditioned SQL generation**.
