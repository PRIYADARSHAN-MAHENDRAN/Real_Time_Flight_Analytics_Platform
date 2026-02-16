
# ✈️ Flight Analytics – End-to-End Data Engineering Project

## 📌 Project Overview

**Flight Analytics** is a real-time aviation data engineering project built on **Databricks** using a **Medallion Architecture (Bronze → Silver → Gold → Mart)**.

The system ingests live flight data from the **OpenSky Network API**, processes it through structured Delta pipelines, and serves curated datasets to **Power BI** for advanced aviation intelligence dashboards.

This project demonstrates:

-   🔐 Secure API ingestion with OAuth
    
-   🏗 Medallion Architecture using Delta Lake
    
-   🔄 Incremental file processing
    
-   📊 Star Schema data modeling
    
-   📈 Business Intelligence integration with Power BI
    
-   ⚙️ Automated Databricks Jobs & Pipelines
    

----------
## 🏗 Architecture

     OpenSky API
       ↓
     Bronze Layer  (Raw JSON - Unity Catalog Volume) 
       ↓
     Bronze Layer  (Raw JSON - Unity Catalog Volume) 
       ↓
     Silver Layer  (Flattened & Cleaned Data) 
       ↓
     Gold Layer  (Star Schema - Fact & Dimensions) 
       ↓
     Mart Layer  (Business KPIs) 
       ↓
     Power BI Dashboard




----------
# 🥉 Bronze Layer – Raw Ingestion

**Notebook:** `bronze.py`

### 🔹 Features

-   Secure credential management using `dbutils.secrets`
    
-   OAuth token generation
    
-   India bounding box filtering
    
-   API data extraction from OpenSky Network
    
-   Raw JSON storage in Unity Catalog Volume
    
-   Time-partitioned storage:
    
    `/Volumes/flight_analytics/bronze/sourcefiles/
        year=YYYY/month=MM/day=DD/` 
    

### 🔹 Purpose

Stores raw, immutable flight state snapshots for audit and replay capability.

----------
# 🥈 Silver Layer – Data Cleaning & Transformation

**Notebook:** `silver.py`

### 🔹 Features

-   Reads Bronze JSON files
    
-   Incremental processing using file tracking table
    
-   Explodes `states` array
    
-   Schema casting and transformation
    
-   Null filtering for essential fields
    
-   Writes structured Delta table
    
-   Maintains processed file tracker
    

### 🔹 Output Table

`flight_analytics.silver.silver_table` 

### 🔹 Incremental Logic

-   Uses `bronze_file_tracker`
    
-   Processes only new JSON files
    
-   Prevents duplicate loads
    

----------
# 🥇 Gold Layer – Star Schema Modeling

**Notebook:** `gold.py`

Implements dimensional modeling.

----------
## 📐 Dimension Tables

### DimAircraft

-   Aircraft ICAO24
    
-   Origin country
    

### DimTime

-   Event timestamp
    
-   Date
    
-   Year / Month / Day / Hour
    

----------
## 📊 Fact Table

### FactFlightSnapshot

Contains:

-   Aircraft key
    
-   Time key
    
-   Longitude & Latitude
    
-   Altitude
    
-   Velocity
    
-   Vertical rate
    
-   On-ground status
    
-   Ingestion timestamp
    

This enables analytical queries and BI reporting.

----------
# 📊 Mart Layer – Business KPIs

**Notebook:** `mart.py`

Creates optimized reporting tables.

----------
## KPI_Hourly

Partitioned by `flight_date`

Includes:

-   Total flights
    
-   Average velocity
    
-   Average altitude
    
-   Flights on ground
    
-   Flights in air
    

----------
## KPI_Aircraft_Activity

-   Snapshot count
    
-   Avg velocity
    
-   Max altitude
    

----------
## KPI_Aircraft_Latest_Position

-   Latest geo position per aircraft
    
-   Used for live geospatial map
    

----------
# 📈 Power BI Dashboards

Power BI connects directly to the **Databricks SQL Endpoint**.

----------
## ✈️ Air Traffic Overview

-   📊 Traffic Volume Over Time
    
-   🚀 Flights vs Average Speed
    
-   📌 Flights In Air %
    
-   🔢 Total Flights
    
-   🕒 Peak Hour Flight Analysis
    

----------
## 🧠 Traffic Intelligence

-   🌍 Geographic Heatmap
    
-   📈 Altitude Trend Line
    
-   🕓 Hourly Flight Distribution
    
-   ⚖️ Air vs Ground Ratio Gauge
    

----------
## 🗺 Live Geospatial Monitoring

-   Real-time aircraft map
    
-   Latest aircraft positions
    
-   Dynamic altitude visualization
    

----------
# ⚙️ Automation

Databricks Jobs & Pipelines:

-   Bronze ingestion scheduled job
    
-   Silver transformation job
    
-   Gold dimensional model job
    
-   Mart aggregation job
    
-   Fully automated daily execution
    

----------
# 🔐 Security

-   Secrets stored in Databricks Secret Scope
    
-   OAuth-based authentication
    
-   Secure API calls
    
-   Unity Catalog governance
    

----------
# 🛠 Technologies Used

-   Databricks
    
-   PySpark
    
-   Delta Lake
    
-   Unity Catalog
    
-   SQL (Delta)
    
-   OpenSky Network API
    
-   Power BI
    
-   OAuth 2.0
    

----------
# 🚀 Key Data Engineering Concepts Demonstrated

-   Medallion Architecture
    
-   Incremental Data Processing
    
-   Delta Lake Transactions
    
-   Star Schema Modeling
    
-   Slowly growing dimensions
    
-   Data Partitioning
    
-   Data Governance
    
-   BI Integration
    
-   Real-time Flight Analytics
