# 🧩 Pinterest Data Pipeline – Local Kafka + Spark Simulation

This project simulates a **Pinterest-style data pipeline** using **Kafka** for event ingestion and **Apache Spark** (via Databricks) for batch data processing and analytics.

It covers a local emulation of real-time user post streaming into Kafka, then performs data cleaning and analytical queries in Spark using batch-loaded CSVs.

---

## 🚀 Project Overview

| Component | Description |
|-----------|-------------|
| `start_kafka.sh` | Starts local Kafka and Zookeeper servers |
| `stop_kafka.sh` | Stops Kafka and Zookeeper |
| `user_posting_emulation.py` | Simulates users posting data to Kafka |
| `batch_processing_spark_databricks.ipynb` | Spark notebook for cleaning and analyzing the dataset |

---

## 🗂️ Architecture

```
User Emulation ──► Kafka Topic ──► (CSV Sink / Static Files)
                                         │
                                ┌────────┴────────┐
                                │  Spark (Databricks)  │
                                └────────┬────────┘
                                         ▼
                             Batch Cleaning & Analytics
```

---

## 🛠️ Prerequisites

- Python 3.7+
- Kafka & Zookeeper installed locally
- Apache Spark or Databricks notebook environment
- Kafka Python client: `pip install kafka-python`
- Input CSVs:
  - `pin_data.csv`
  - `geo_data.csv`
  - `user_data.csv`

---

## 🧪 Running the Simulation Locally

### 1️⃣ Start Kafka & Zookeeper

```bash
chmod +x start_kafka.sh
./start_kafka.sh
```

> This script launches both Kafka and Zookeeper on default ports (2181 & 9092).

---

### 2️⃣ Simulate User Posts to Kafka

```bash
python user_posting_emulation.py
```

- Simulates users posting pin data as Kafka messages (JSON format).
- You can optionally modify the script to write to `pin_data.csv` for downstream batch processing.

---

### 3️⃣ Batch Processing in Spark (Databricks)

1. Open `batch_processing_spark_databricks.ipynb` in **Databricks**
2. Upload the CSVs to `/FileStore/tables/`:
   - `pin_data.csv`
   - `geo_data.csv`
   - `user_data.csv`
3. Run the notebook cells sequentially to:
   - Clean and transform raw data
   - Join across datasets
   - Perform analytical queries

---

## 🧹 Data Cleaning Summary

| Dataset | Key Cleaning Steps |
|---------|--------------------|
| `df_pin` | Normalize `follower_count`, clean `save_location`, convert data types, rename/reorder columns |
| `df_geo` | Merge `latitude` & `longitude` into `coordinates` array, cast `timestamp` |
| `df_user` | Create `user_name`, cast `date_joined`, define `age_group` |

---

## 📊 Analytics Performed

- 📌 Most popular **category per country**
- 📅 Category post counts from **2018 to 2022**
- 🌍 Most-followed user **per country**
- 👥 Popular categories by **age group** (18–24, 25–35, 36–50, 50+)
- 📈 Median follower count by **age group** and **year joined**
- 📆 Number of users joined from **2015 to 2020**

---

## 📂 Project Structure

```
📁 pinterest-pipeline/
├── start_kafka.sh
├── stop_kafka.sh
├── user_posting_emulation.py
├── batch_processing_spark_databricks.ipynb
├── pin_data.csv
├── geo_data.csv
└── user_data.csv
```

---

## 🧨 Stop Kafka & Zookeeper

```bash
./stop_kafka.sh
```

> Gracefully shuts down the Kafka and Zookeeper services.

---

## 📜 License

MIT License. Free to use for learning and educational purposes.

---

## 🙋‍♀️ Author & Credits

This simulation was built to demonstrate authors skills in building hybrid data pipelines using real-time Kafka ingestion and batch analytics with Spark. Ideal for learning how modern data platforms work.
