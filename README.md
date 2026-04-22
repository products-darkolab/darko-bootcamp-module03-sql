# Module 03 — SQL and PostgreSQL
## The Darko Method 2026 | Teaching Project

---

## The Business Problem

**Company:** GlobalTech Solutions
**Team:** People Analytics
**Your role:** Data Analyst

GlobalTech has 1,000 employees stored in a PostgreSQL database on Supabase. The data lives across three separate tables — employees, sales, and customers — because that is how databases are designed: each table stores one type of thing, and related data is connected by shared ID columns. This is efficient for storage, but it means no single table tells the complete story of an employee. You cannot look at the employees table alone and know how much revenue Alice generated last quarter, because that information lives in the sales table.

The People Analytics team has been given a mandate from the VP of HR: produce a weekly workforce report that combines employee profiles with their sales performance. Before anyone can run machine learning models or build dashboards, someone has to extract the raw data from the database and get it into a format that Python can work with. That someone is you. Your job in this module is to write the SQL queries that pull data from these tables, join them together, and produce a single flat file called `raw-data.csv`. This file becomes the starting point for every module that follows — Module 05 cleans it, Module 06 analyses it, Module 09 trains a model on it. If the extraction is wrong, everything downstream is wrong.

One more thing: the data in the database is intentionally messy. The seed scripts introduced null salaries, negative values, impossible years of experience, and duplicate records. This was deliberate. In the real world, databases are never clean — they accumulate errors from data entry mistakes, system migrations, and edge cases nobody anticipated. When you open `raw-data.csv`, you will see those problems. That is not a bug in your code. That is the authentic reality of working with production data, and it is exactly why Module 05 exists.

---

**Job 2 — Extract `raw-data.csv`:**

Once you are comfortable with SQL, run `05_extract_raw_data.sql`. This joins employees, sales, and customers into one flat file and saves it to `data/raw-data.csv`. This is the real deliverable. Every downstream module depends on it.

---
## Project Structure

```
teaching-project/
├── .env.example           ← copy to .env and add your credentials
├── .gitignore             ← .env and data/*.csv are excluded from Git
├── requirements.txt
├── config.py              ← reads from .env, never stores credentials
├── run.py                 ← runs the extraction and saves raw-data.csv
├── sql/
│   └── 05_extract_raw_data.sql
├── src/
│   ├── query_runner.py    ← executes SQL and returns DataFrames
│   └── data_extractor.py  ← runs the production extraction query
├── data/
│   └── .gitkeep           ← keeps the folder in Git; raw-data.csv is not committed
└── tests/
    └── test_sql.py
```
## After This Module

You have learned SQL and produced the raw data file. Now use what you learned to complete your three personal projects:

- `P01-healthcare-sql` ⭐ — write SQL for the healthcare schema
- `P02-manufacturing-sql` ⭐⭐ — write SQL for the manufacturing schema
- `P03-ecommerce-sql` ⭐⭐⭐ — write SQL for the ecommerce schema

Each project has the same structure as this teaching project. Your task is to write the SQL files from scratch for your assigned industry, following exactly what we did together here.

---

## Output

| File | Where it goes |
|---|---|
| `data/raw-data.csv` | Input to Module 05 ETL |