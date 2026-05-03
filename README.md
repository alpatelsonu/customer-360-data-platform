# 🧠 Customer 360 Data Platform (Snowflake + dbt + Airflow)

## 📌 Overview

This project builds a **Customer 360 data platform** to solve a real-world problem:

> **Fragmented customer identities lead to incorrect business metrics like DAU, LTV, and retention.**

Using **Snowflake, dbt, and Airflow**, this pipeline:

- Resolves multi-hop customer identity (A → B → C)
- Tracks identity evolution using **SCD Type 2**
- Enables **point-in-time correct analytics**
- Produces business-ready metrics (LTV, retention, cohorts)

---

## 🏗️ Architecture (docs/architecture.png)

```
RAW → BRONZE → SILVER → SNAPSHOT → GOLD
          ↓
     Identity Resolution
```

### Data Flow

```
RAW (Snowflake)
  ├── user_events (JSON)
  ├── orders (CSV)
  └── identity_map (CSV)

        ↓

BRONZE
  - Data typing
  - Deduplication
  - Basic validation

        ↓

SILVER
  - identity_current (latest mapping)
  - user_events enrichment (latest + PIT)

        ↓

SNAPSHOT (dbt SCD2)
  - identity_resolved
  - dbt_valid_from / dbt_valid_to

        ↓

GOLD
  - customer_metrics (LTV, AOV, segmentation)
  - cohort_retention
  - retention_heatmap

        ↓

AIRFLOW
  - Orchestration (run → snapshot → downstream → test)
```

---

## 🔑 Core Problem: Identity Fragmentation

A single customer may appear as multiple IDs:

```
A → B → C
```

Without resolution:

- ❌ DAU is inflated
- ❌ LTV is fragmented
- ❌ Retention is incorrect

---

## ✅ Solution Approach

### 1️⃣ Identity Resolution (Latest State)

- Recursive SQL resolves multi-hop identity chains
- Produces:

```
user_id → unified_customer_id
```

Example:

```
A → B → C  →  A, B, C → C
```

---

### 2️⃣ Temporal Tracking (SCD Type 2)

Implemented using **dbt snapshots**

Tracks identity changes over time:

| user_id | unified_customer_id | valid_from | valid_to |
| ------- | ------------------- | ---------- | -------- |

👉 Enables **point-in-time joins**

---

### 3️⃣ Event Enrichment

Two approaches:

| Model                       | Purpose              |
| --------------------------- | -------------------- |
| `silver_user_events_latest` | Fast, current-state  |
| `silver_user_events_pit`    | Historically correct |

---

## 📊 Gold Layer (Business Metrics)

### Customer Metrics

- Total Revenue
- Lifetime Value (LTV)
- Average Order Value (AOV)
- Repeat Rate
- Customer Segmentation (High / Mid / Low)

---

### Cohort Analysis

Tracks retention over time:

```
Cohort = first activity month
Retention = % users returning in future months
```

---

### Retention Heatmap

| Cohort ↓ / Month → | M0  | M1  | M2   | M3   |
| ------------------ | --- | --- | ---- | ---- |
| Jan                | 1.0 | 0.4 | 0.35 | 0.34 |

---

## ⚙️ Orchestration (Airflow)

DAG execution order:

```
dbt run -s tag:upstream
→ dbt snapshot
→ dbt run -s tag:downstream
→ dbt test
```

👉 Ensures:

- Snapshot is refreshed before PIT models
- Correct temporal joins

---

## 🧠 Key Design Decisions

### ✔ Snapshot Strategy

- Used `timestamp` strategy
- Aligns identity changes with **business time**, not system time

---

### ✔ Identity Modeling

- Latest + historical (SCD2)
- Supports both fast dashboards and correct analytics

---

### ✔ PIT Join Handling

- Left join with fallback to latest identity (to handle gaps)

---

## ⚖️ Tradeoffs

| Decision                      | Reason                             |
| ----------------------------- | ---------------------------------- |
| Recursive identity (vs graph) | Simpler, sufficient for e-commerce |
| Full refresh models           | Easier for demo / portfolio        |
| Snapshot-based SCD2           | Native dbt support                 |

---

## ⚠️ Known Limitations

- Simulated data may create **temporal gaps**
- PIT joins may produce NULLs if identity is missing
- No incremental models (yet)
- No streaming ingestion (batch only)

---

## 📂 Project Structure

```
models/
  bronze/
  silver/
  gold/

snapshots/
  silver_identity_resolved.sql

airflow/
  dags/
    customer360_dag.py
```

---

## 🛠️ Tech Stack

- Snowflake
- dbt (core + snapshots)
- Apache Airflow (3.x)
- SQL

---

## 💡 Why This Project Matters

This project demonstrates:

- Identity resolution (real-world problem)
- SCD Type 2 modeling
- Temporal joins (PIT correctness)
- Data modeling best practices
- End-to-end orchestration

---

## 🔮 Future Improvements

- Incremental models
- Kafka/Flink streaming pipeline
- Dockerized Airflow
- Dashboard (Streamlit / Superset)
- Graph-based identity resolution

---

## 👤 Sonu Patel : alpatelsonu@gmail.com

Built as a production-inspired data engineering portfoli
