# LLM Access Control Datasets

This repository provides two large-scale datasets and companion scripts for studying permission-aware reasoning in Large Language Models (LLMs). The datasets extend popular text-to-SQL benchmarks, Spider and BIRD, with role-based access control (RBAC) metadata, enabling systematic evaluation of models that must decide whether a user’s SQL query should be *permitted* or *denied* under defined access policies.

# Citation

If you use these datasets in your research, please cite the following paper:
```bibtex
@article{klisura2025role,
  title={Role-Conditioned Refusals: Evaluating Access Control Reasoning in Large Language Models},
  author={Klisura, {\DJ}or{\dj}e and Khoury, Joseph and Kundu, Ashish and Krishnan, Ram and Rios, Anthony},
  journal={arXiv preprint arXiv:2510.07642},
  year={2025}
}
```
---

## Repository Structure

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
│   
│
├── requirements.txt              # Python dependencies
└── README.md                     # You are here
```

---

## Dataset Overview

| Dataset | # Databases | # (User, Query) Pairs | # Permitted | # Denied | Size | 
|----------|-------------|----------------------|--------------|-----------|-------|
| **Spider** | 153 | 19.6K | 8K | 11K | ~9 MB | 
| **BIRD** | 80 | 35.8K | 11.9K | 23.9K | ~220 MB | 

Each record includes:
- `question` — natural-language query  
- `sql` / `sql_wrapped` — executable SQL  
- `policy_sql` — role-based GRANT statements  
- `schema_ddl` — database schema (introspected from PostgreSQL)  
- `user` — simulated role (`User_1`–`User_4`)  
- `decision` — `PERMIT` or `DENY` (ground truth)  
- `sqlstate` and `error` — PostgreSQL response code and message  

---

## Roles and Policies

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

## Environment Setup

```bash
git clone https://github.com/<your-org-or-user>/LLM-Access-Control-Datasets.git
cd LLM-Access-Control-Datasets
pip install -r requirements.txt
```

Python ≥3.9 is recommended.


## License

Released under the **MIT License**.  
The original Spider and BIRD datasets are distributed under their respective academic research licenses.

---

## Acknowledgments

This repository builds upon the following benchmark datasets:

- **Spider** — developed by the Yale NLP Group, a large-scale human-labeled dataset for complex and cross-domain Text-to-SQL tasks (Yu et al., 2018).  
- **BIRD** — a large-scale database-grounded Text-to-SQL benchmark designed to evaluate large language models as database interfaces (Li et al., 2024).  

We thank the creators of these datasets for making their resources publicly available, which enabled this extension on access-control–aware Text-to-SQL reasoning.

### References

```bibtex
@inproceedings{yu2018spider,
  title     = {Spider: A Large-Scale Human-Labeled Dataset for Complex and Cross-Domain Semantic Parsing and Text-to-SQL Task},
  author    = {Yu, Tao and Zhang, Rui and Yang, Kai and Yasunaga, Michihiro and Wang, Dongxu and Li, Zifan and Ma, James and Li, Irene and Yao, Qingning and Roman, Shanelle and Zhang, Zilin and Radev, Dragomir},
  booktitle = {Proceedings of the 2018 Conference on Empirical Methods in Natural Language Processing (EMNLP)},
  year      = {2018}
}

@article{li2024can,
  title   = {Can LLM Already Serve as a Database Interface? A Big Bench for Large-Scale Database Grounded Text-to-SQLs},
  author  = {Li, Jinyang and Hui, Binyuan and Qu, Ge and Yang, Jiaxi and Li, Binhua and Li, Bowen and Wang, Bailin and Qin, Bowen and Geng, Ruiying and Huo, Nan and others},
  journal = {Advances in Neural Information Processing Systems},
  volume  = {36},
  year    = {2024}
}
```
