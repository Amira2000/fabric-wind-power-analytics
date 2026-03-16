# fabric-wind-power-analytics
# 🌬️ End-to-End Wind Power Data Platform with Microsoft Fabric

## 📌 Project Overview

This project demonstrates the design and implementation of a **modern end-to-end data engineering platform using Microsoft Fabric**.

The platform ingests wind turbine telemetry data, processes it through a **Lakehouse architecture**, and exposes the final curated dataset through an interactive **Power BI dashboard**.

The system simulates a real-world **data engineering workflow**, including:

- automated data ingestion
- Spark-based transformations
- lakehouse storage
- dimensional data modeling
- pipeline orchestration
- business intelligence reporting

The platform processes **wind turbine electricity production data generated every 10 minutes**, transforming raw telemetry into actionable analytics.

---

# 🏗️ General Architecture

This project follows the **Medallion Architecture (Bronze → Silver → Gold)** used in modern data lakehouse platforms.

- **Bronze Layer** → Raw data ingestion  
- **Silver Layer** → Data cleaning and transformation  
- **Gold Layer** → Analytics-ready star schema
- 
![Image Alt](https://github.com/Amira2000/fabric-wind-power-analytics/blob/3aca66960f8b09afda8aa2ee5a024425c4256140/capture%201.png)

The pipeline ensures that the **entire data platform runs automatically on a scheduled basis**.

---

# 🗂️ Data Model and Layers

The data platform uses a **star schema model** to optimize analytical queries and reporting performance.

### Fact Table

**FactWindPower**

Stores measurable metrics such as:

- energy produced
- wind speed
- wind direction

### Dimension Tables

The dataset includes several dimensions that enrich the fact table:

- **dim_date** → calendar information
- **dim_time** → time attributes
- **dim_turbine** → turbine metadata
- **dim_operational_status** → turbine operational state

Paste your **Star Schema / Semantic Model image here**






The dashboard updates automatically whenever the pipeline processes new data.

---

# 🧰 Technologies Used

- Microsoft Fabric  
- PySpark  
- Spark SQL  
- Delta Lake  
- Power BI  
- Fabric Data Pipelines  
- Dataflow Gen2  
- GitHub  

---

# 🚀 Key Skills Demonstrated

This project demonstrates key **data engineering capabilities**, including:

- End-to-End Data Pipeline Design
- Lakehouse Architecture
- Distributed Data Processing with Spark
- Dimensional Data Modeling
- Automated Data Orchestration
- Business Intelligence Reporting

---
