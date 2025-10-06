# LLM Access Control Datasets

This repository provides **two large-scale datasets** and companion scripts for studying **permission-aware reasoning in Large Language Models (LLMs)**.  
The datasets extend popular text-to-SQL benchmarks — **Spider** and **BIRD** — with **role-based access control (RBAC)** metadata, enabling systematic evaluation of models that must decide whether a user’s SQL query should be *permitted* or *denied* under defined access policies.

---

## 📂 Repository Structure

```
LLM-Access-Control-Datasets/
│
├── data/
│   ├── spider/                   # Spider access-control dataset (JSONL + SCHEMA.md)
│   └── bird/                     # BIRD access-control dataset (JSONL + SCHEMA.md)
│
├── scripts/
│   ├── spider/                   # Generation pipeline for Spider
│   └── bird/                     # Generation pipeline for BIRD
│
├── docker/                       # Docker stack for reproducing Postgres instances
│   ├── docker-compose.yml
│   └── .env.example
│
├── requirements.txt              # Python dependencies
└── README.md                     # You are here
```

---

## 📘 Dataset Overview

| Dataset | # Databases | # (User, Query) Pairs | # Permitted | # Denied | Size | 
|----------|-------------|----------------------|--------------|-----------|-------|
| **Spider** | 166 | ~9.6K | ≈2.8K | ≈6.8K | ~9 MB | 
| **BIRD** | 95 | 43.8K → 35.8K | 11.9K | 23.9K | ~220 MB | 

Each record includes:
- `question` — natural-language query  
- `sql` / `sql_wrapped` — executable SQL  
- `policy_sql` — role-based GRANT statements  
- `schema_ddl` — database schema (introspected from PostgreSQL)  
- `user` — simulated role (`User_1`–`User_4`)  
- `decision` — `PERMIT` or `DENY` (ground truth)  
- `sqlstate` and `error` — PostgreSQL response code and message  

---

## 🧩 Roles and Policies

Each database includes **four user roles** representing realistic organizational access tiers:

| Role | Description |
|------|--------------|
| `User_1` | Full read access (admin/senior analyst) |
| `User_2` | Limited tables (department-level) |
| `User_3` | Full tables but restricted columns (masked sensitive data) |
| `User_4` | Minimal visibility (external/junior role) |

Permissions are defined through automatically generated SQL `GRANT` statements reflecting **table- and column-level constraints**.  
Ground truth decisions are computed by executing each query under the user’s role in PostgreSQL and logging the resulting outcome.

---

## ⚙️ Environment Setup

```bash
git clone https://github.com/<your-org-or-user>/LLM-Access-Control-Datasets.git
cd LLM-Access-Control-Datasets
pip install -r requirements.txt
```

Python ≥3.9 is recommended.

---

## 🐘 Running with Docker

You can spin up both the **Spider** and **BIRD** PostgreSQL instances using Docker Compose:

```bash
cd docker
cp .env.example .env
# (optional) edit .env to set dataset mount paths and credentials
docker compose up -d
```

Access via:
- **Spider Postgres:** `localhost:5432`
- **BIRD Postgres:** `localhost:5433`
- **pgAdmin UI:**  
  - Spider → [http://localhost:5050](http://localhost:5050)  
  - BIRD → [http://localhost:5051](http://localhost:5051)

---

All outputs are written into `/data/<dataset>/`.

## 🔐 License

Released under the **MIT License**.  
The original Spider and BIRD datasets are distributed under their respective academic research licenses.

---

## 🧾 Acknowledgments

This repository builds upon:
- **Spider** — Yale NLP Group (Yu et al., 2018)  
- **BIRD** — Singapore Management University (Liang et al., 2023)  

Access-control extensions, policies, and ground-truth computations were developed by **Đorđe Klisura** (UTSA, 2025).

---

*For questions or contributions, please open an issue or pull request.*
