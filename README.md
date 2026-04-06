# 🚕 NYC Taxi ETL Pipeline (Databricks + PySpark)

## 📌 Overview

Built an end-to-end ETL pipeline using PySpark on Databricks, implementing Medallion Architecture (Bronze → Silver → Gold) to process and analyze NYC Taxi data.

---

## 🧱 Architecture

Bronze (Raw Data) → Silver (Cleaned Data) → Gold (Aggregated Data)

---

## 🛠️ Tech Stack

* PySpark
* Databricks
* Delta Lake

---

## ⚙️ Pipeline Steps

### 🥉 Bronze Layer

* Raw NYC Taxi dataset ingested as a Delta table

### 🥈 Silver Layer

* Removed invalid records (nulls, negative values)
* Feature engineering:

  * Trip duration calculation
* Filtered unrealistic trips

### 🥇 Gold Layer

* Aggregated business metrics:

  * Daily revenue
  * Average trip distance
  * Total trips

---

## 🚀 Optimizations

* Partitioned Gold table by `pickup_date`
* Improved query performance using Delta Lake optimizations

---

## 📊 Sample Output

### Bronze Layer
![Bronze](screenshots/bronze_table.png)

### Silver Layer
![Silver](screenshots/silver_table.png)

### Gold Layer
![Gold](screenshots/gold_table.png)

### Final Output
![Output](screenshots/output.png)

---

## 🧠 Key Learnings

* Handling real-world dirty data
* Building scalable ETL pipelines
* Implementing Medallion Architecture
* Optimizing data storage using Delta Lake

---

## 🚀 Future Improvements

* Add streaming pipeline using Spark Structured Streaming
* Implement orchestration using Airflow
