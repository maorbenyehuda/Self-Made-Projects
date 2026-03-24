# 🎮 End-to-End Gaming Data Platform (Kafka → Warehouse → ML → Dashboard)

## 📌 Overview

This project demonstrates a full **end-to-end data platform** simulating a real-world gaming system.

It includes:

* Event generation (players, matches, sessions)
* Streaming with Kafka
* Data modeling (Bronze → Silver → Gold)
* Business KPI computation
* Feature engineering for Machine Learning
* Churn prediction model
* Interactive dashboard (Streamlit)

The goal is to showcase **real Data Engineering + Analytics + ML workflow**.

---

## 🧱 Architecture

```
Simulator → Kafka → Silver (Postgres) → Gold → Features → ML → Dashboard
```

### Flow:

1. **Simulator** generates game events
2. Events are sent to **Kafka topics**
3. **Consumer (Silver Processor)** writes normalized data into Postgres
4. SQL builds **Gold KPI tables**
5. SQL builds **feature table for ML**
6. ML model predicts churn
7. Streamlit dashboard visualizes KPIs

---

## ⚙️ Tech Stack

* Python
* Apache Kafka (Docker)
* PostgreSQL
* SQL
* Pandas / Scikit-learn / XGBoost
* Streamlit (Dashboard)

---

## 📊 Data Layers

### 🔹 Silver (Fact Tables)

* `fact_sessions`
* `fact_matches`
* `fact_match_players`
* `fact_rewards`
* `fact_purchases`
* `fact_levelups`

---

### 🔸 Gold (KPIs)

* Daily Active Users (DAU)
* Session Length
* Purchase Rate
* Matches per Player
* Match Completion Rate
* Early Exit Rate
* Match Balance Score
* Progression Speed
* Retention (D1, D7)

---

### 🧠 Features (ML)

Table: `gold_features_player_day`

Includes:

* Sessions / playtime
* Matches / wins / losses
* Early exits
* XP / gold
* Purchases
* Level progression
* Churn label

---

## 🧠 Churn Definition

```text
churn = no activity in the next 2 days
```

*(Adjusted to match simulation behavior)*

---

# 🚀 Full Pipeline Execution (Step-by-Step)

Run everything from project root:

```text
C:\...\Data_Project
```

---

## 0) Full Reset (optional)

```bash
docker compose down -v
docker compose up -d
```

---

## 1) Create Kafka Topics

```bash
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --create --topic player-events --partitions 3 --replication-factor 1

docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --create --topic match-events --partitions 3 --replication-factor 1
```

Check:

```bash
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --list
```

---

## 2) Create Tables (Silver + Gold + Features)

```bash
Get-Content .\sql\create_silver_tables.sql | docker exec -i pg psql -U app -d eventsdb

Get-Content .\sql\create_gold_tables.sql | docker exec -i pg psql -U app -d eventsdb

Get-Content .\sql\create_gold_features.sql | docker exec -i pg psql -U app -d eventsdb
```

---

## 3) Generate Events (JSON files)

```bash
python run_generate.py
```

---

## 4) Produce Events to Kafka

```bash
python src\kafka\producer.py
```

---

## 5) Run Silver Consumer (Postgres ingestion)

```bash
python src\kafka\silver_processor.py
```

When the consumer becomes idle → stop with:

```bash
Ctrl + C
```

---

## 6) Build Gold KPIs

```bash
Get-Content .\sql\build_gold.sql | docker exec -i pg psql -U app -d eventsdb
```

---

## 7) Build Gold Features (for ML)

```bash
Get-Content .\sql\build_gold_features.sql | docker exec -i pg psql -U app -d eventsdb
```

---

## 8) Quick Validation (Postgres)

```bash
docker exec -it pg psql -U app -d eventsdb
```

Run:

```sql
SELECT * FROM gold_daily_active_users;
SELECT * FROM gold_session_length;
SELECT * FROM gold_purchase_rate;
SELECT * FROM gold_match_completion;
SELECT * FROM gold_early_exit_rate;
SELECT * FROM gold_match_balance;
SELECT * FROM gold_progression_speed;
SELECT * FROM gold_features_player_day LIMIT 20;
```

---

# 🤖 Machine Learning

Run the churn model:

```bash
jupyter notebook churn_prediction.ipynb
```

Inside the notebook:

* Load `gold_features_player_day`
* Train models (LogReg / XGBoost)
* Evaluate performance
* Analyze feature importance

---

# 📊 Dashboard (Streamlit)

Run:

```bash
streamlit run streamlit_dashboard.py
```

The dashboard includes:

* Daily KPI trends
* Retention analysis
* Churn feature insights
* Player-level exploration

---

# ⚖️ Design Decisions

* **Kafka** → scalable streaming & decoupling
* **Partition by player_id** → ordering guarantee per player
* **Postgres as warehouse** → simple & realistic
* **Silver layer** → normalized, analytics-ready
* **Gold layer** → business KPIs
* **Feature layer** → ML-ready dataset

---

# Key Learnings:
- Built streaming pipeline with Kafka
- Designed normalized data warehouse (Silver)
- Created business KPIs (Gold layer)
- Engineered features for ML
- Built churn prediction model

---

# 🚀 Future Improvements

* Real-time prediction service
* Feature store
* Better behavioral simulation
* Monitoring (Kafka lag, errors)
* Advanced dashboard (filters, drilldowns)

---

## 📎 Summary

This project demonstrates a full modern data pipeline:

```text
Events → Streaming → Storage → Modeling → Analytics → ML → Visualization
```

It reflects real-world Data Engineering and Data Science workflows end-to-end.
