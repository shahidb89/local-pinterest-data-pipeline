
# Pinterest Data Pipeline – Local Kafka + Spark Simulation

This project simulates a Pinterest-style data pipeline that uses Kafka for event ingestion and Apache Spark (via Databricks) for batch data processing and analytics.  
It demonstrates a complete workflow:

1. **Real-time** streaming of user activity into Kafka via a FastAPI service.  
2. **Batch** extraction of aligned records from Kafka into CSV files.  
3. **Spark**-based cleaning, transformation, and exploratory analytics.

---

## Project Overview

| Component | Description |
|-----------|-------------|
| `start_kafka.sh` | Starts local Kafka and Zookeeper services |
| `stop_kafka.sh`  | Stops Kafka and Zookeeper |
| `data_poster.py` | Streams random records from PostgreSQL to Kafka topics in real time (runs a FastAPI server) |
| `batch_extractor.py` | Consumes Kafka messages, aligns them by `idx`, and writes three CSV files (pin, geo, user) |
| `batch_processing_spark_databricks.ipynb` | Databricks/Spark notebook that cleans, joins, and analyzes the extracted data |

---

## Architecture

```
                +----------------+
                |  data_poster.py|
                |  (FastAPI)     |
                +--------+-------+
                         |
                         | HTTP
                         v
+----------------+   Kafka Topics   +--------------------+
|  PostgreSQL    | ---------------> | pin_data.pin       |
|  pinterest_*   |                  | pin_data.geo       |
+----------------+                  | pin_data.user      |
                                    +---------+----------+
                                              |
                                              | batch_extractor.py
                                              v
                                  +-----------+-------------+
                                  |  CSV Sink (pin/geo/user)|
                                  +-----------+-------------+
                                              |
                                              v
                                   Spark / Databricks Notebook
                                      (cleaning & analytics)
```

---

## Prerequisites

- Python 3.7 or newer  
- Kafka and Zookeeper installed locally  
- Apache Spark or a Databricks workspace  
- Python packages listed in `requirements.txt`

Install all Python dependencies:

```bash
pip install -r requirements.txt
```

---

## Running the Simulation Locally

### 1. Start Kafka and Zookeeper

```bash
chmod +x start_kafka.sh
./start_kafka.sh
```

Kafka defaults:  
* Zookeeper → port 2181  
* Kafka broker → port 9092

---

### 2. Stream Real-Time User Posts

```bash
python data_poster.py
```

- Launches FastAPI on `http://localhost:8000`  
- Pulls random records from PostgreSQL and posts them to Kafka topics

---

### 3. Extract Aligned Kafka Messages

```bash
python batch_extractor.py
```

- Subscribes to `pin_data.pin`, `pin_data.geo`, and `pin_data.user`  
- Collects 500 complete triplets (matched by `idx`)  
- Writes results to `batch_data/`:

  ```
  batch_data/pin_data.csv
  batch_data/geo_data.csv
  batch_data/user_data.csv
  ```

---

### 4. Process Data in Spark

1. Open `batch_processing_spark_databricks.ipynb` in Databricks.  
2. Upload the three CSV files to `/FileStore/tables/`.  
3. Run the notebook cells to clean, join, and analyze the data.

---

## Data Cleaning Summary

| Dataset | Key Cleaning Steps |
|---------|--------------------|
| `df_pin`  | Normalize `follower_count`, clean `save_location`, parse dates, rename columns |
| `df_geo`  | Merge latitude/longitude into a coordinate array, cast timestamps |
| `df_user` | Standardize `date_joined`, generate `user_name`, assign `age_group` |

---

## Analytics Performed

- Most popular category per country  
- Category post counts (2018 – 2022)  
- Most-followed user per country  
- Popular categories by age group (18–24, 25–35, 36–50, 50+)  
- Median follower count by age group and year joined  
- User join trends from 2015 – 2020  

---

## Project Structure

```
pinterest-pipeline/
├── start_kafka.sh
├── stop_kafka.sh
├── data_poster.py
├── batch_extractor.py
├── batch_processing_spark_databricks.ipynb
├── requirements.txt
└── batch_data/
    ├── pin_data.csv
    ├── geo_data.csv
    └── user_data.csv
```

---

## Stopping Kafka and Zookeeper

```bash
./stop_kafka.sh
```

Shuts down both services gracefully.

---

## License

This project is released under the MIT License.  
Free to use for learning, testing, and educational purposes.

---

## Author & Credits

Created by **Shahid Hadi** to demonstrate real-time and batch data-engineering techniques:

- Kafka-driven event ingestion  
- FastAPI microservices  
- Databricks/Spark batch analytics  

Contributions and feedback are welcome.
