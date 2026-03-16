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
