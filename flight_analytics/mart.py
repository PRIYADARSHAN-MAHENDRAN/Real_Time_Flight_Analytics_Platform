# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS flight_analytics.mart.KPI_Hourly (
# MAGIC     flight_date DATE,
# MAGIC     hour INT,
# MAGIC     total_flights BIGINT,
# MAGIC     avg_velocity DOUBLE,
# MAGIC     avg_altitude DOUBLE,
# MAGIC     flights_on_ground BIGINT,
# MAGIC     flights_in_air BIGINT
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (flight_date);
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT OVERWRITE flight_analytics.mart.KPI_Hourly
# MAGIC SELECT
# MAGIC     t.flight_date,
# MAGIC     t.hour,
# MAGIC     COUNT(*) AS total_flights,
# MAGIC     AVG(f.velocity) AS avg_velocity,
# MAGIC     AVG(f.geo_altitude) AS avg_altitude,
# MAGIC     SUM(CASE WHEN f.on_ground THEN 1 ELSE 0 END) AS flights_on_ground,
# MAGIC     SUM(CASE WHEN NOT f.on_ground THEN 1 ELSE 0 END) AS flights_in_air
# MAGIC FROM flight_analytics.gold.FactFlightSnapshot f
# MAGIC JOIN flight_analytics.gold.DimTime t
# MAGIC   ON f.DimTimeKey = t.DimTimeKey
# MAGIC WHERE t.flight_date = current_date()   -- only today's partition
# MAGIC GROUP BY t.flight_date, t.hour;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE flight_analytics.mart.KPI_Aircraft_Activity AS
# MAGIC SELECT
# MAGIC     a.icao24,
# MAGIC     COUNT(*) AS snapshot_count,
# MAGIC     AVG(f.velocity) AS avg_velocity,
# MAGIC     MAX(f.geo_altitude) AS max_altitude
# MAGIC FROM flight_analytics.gold.FactFlightSnapshot f
# MAGIC JOIN flight_analytics.gold.DimAircraft a
# MAGIC   ON f.DimAircraftKey = a.DimAircraftKey
# MAGIC GROUP BY a.icao24;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE flight_analytics.mart.KPI_Aircraft_Latest_Position AS
# MAGIC SELECT *
# MAGIC FROM (
# MAGIC     SELECT
# MAGIC         a.icao24,
# MAGIC         f.longitude,
# MAGIC         f.latitude,
# MAGIC         f.geo_altitude,
# MAGIC         f.velocity,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY a.icao24
# MAGIC             ORDER BY f.ingestion_time DESC
# MAGIC         ) AS rn
# MAGIC     FROM flight_analytics.gold.FactFlightSnapshot f
# MAGIC     JOIN flight_analytics.gold.DimAircraft a
# MAGIC       ON f.DimAircraftKey = a.DimAircraftKey
# MAGIC )
# MAGIC WHERE rn = 1;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from flight_analytics.mart.kpi_hourly