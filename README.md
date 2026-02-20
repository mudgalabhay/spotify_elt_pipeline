# Spotify Modern Data Stack Project

![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?logo=snowflake&logoColor=white)
![DBT](https://img.shields.io/badge/dbt-FF694B?logo=dbt&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?logo=apacheairflow&logoColor=white)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-231F20?logo=apachekafka&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?logo=python&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?logo=docker&logoColor=white)
![Modern Data Stack](https://img.shields.io/badge/Modern%20Data%20Stack-00C7B7?logo=databricks&logoColor=white)

---

## 📌 Project Overview

This project demonstrates an **end-to-end real-time data engineering pipeline** for **Spotify music analytics** using the **Modern Data Stack (MDS)**.  
We simulate streaming music data — including **song plays, listeners, regions, and device types** — and build a fully automated pipeline from **data ingestion to analytics-ready datasets**.

Below components run in the pipeline:  
data simulation → streaming via Kafka → storage in Snowflake → transformation with DBT.

👉 Think of it as a **real-world Spotify analytics system** built on top of cutting-edge data tools.

---

## 🏗️ Architecture

<img width="891" height="522" alt="image" src="https://github.com/user-attachments/assets/d0c03b60-c08d-4357-b56b-96312aed1881" />


**Pipeline Flow:**
1. **Data Simulator** → Generates fake Spotify streaming data (user, track, region, device).  
2. **Kafka Producer** → Streams the data to Kafka topics in real time.  
3. **Kafka Consumer** → Consumes and stores the raw data into **MinIO (S3-compatible storage)**.  
4. **Airflow** → Orchestrates data loading from MinIO → Snowflake (Bronze).  
5. **Snowflake** → Stores and manages data in **Bronze → Silver → Gold layers**.  
6. **DBT** → Cleans, transforms, and builds analytics-ready models directly inside Snowflake.  

---

## ⚡ Tech Stack

- **Python (Faker)** → Data simulation  
- **Apache Kafka** → Real-time data streaming  
- **MinIO** → Object storage (S3-compatible)  
- **Snowflake** → Cloud data warehouse  
- **DBT** → Transformations, tests, and models  
- **Apache Airflow** → Orchestration and DAG scheduling  
- **Docker & docker-compose** → Containerized environment  

---

## ✅ Key Features

- **Fully automated pipeline** — end-to-end from ingestion to analytics-ready data  
- **Real-time streaming** using Kafka  
- **Medallion Architecture (Bronze → Silver → Gold)** implemented in Snowflake  
- **DBT for transformation and testing** (clean, modular SQL models)  
- **Containerized deployment** for reproducibility  
- **CI/CD pipeline** with dbt test automation  

---

## 📂 Repository Structure

```text
spotify-mds-pipeline/
├── docker/
│   ├── .env
│   ├── docker-compose.yml
│   └── dags/
│       ├── minio-to-kafka.py
│       └── .env
├── spotify_dbt/
│   └── models/
│       ├── gold/
│       ├── silver/
│       └── sources.yml
├── simulator/
│   ├── producer.py
│   └── .env
├── consumer/
│   ├── kafka-to-minio.py
│   └── .env
├── docker-compose.yml
├── requirements.txt
└── README.md
