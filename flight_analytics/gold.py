# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS flight_analytics.gold.DimAircraft (
# MAGIC     DimAircraftKey BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC     icao24 STRING,
# MAGIC     origin_country STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO flight_analytics.gold.DimAircraft AS target
# MAGIC USING (
# MAGIC     SELECT DISTINCT icao24, origin_country
# MAGIC     FROM flight_analytics.silver.silver_table
# MAGIC ) AS source
# MAGIC ON target.icao24 = source.icao24
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT (icao24, origin_country)
# MAGIC VALUES (source.icao24, source.origin_country);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS flight_analytics.gold.DimTime (
# MAGIC     DimTimeKey BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC     event_time TIMESTAMP,
# MAGIC     flight_date DATE,
# MAGIC     year INT,
# MAGIC     month INT,
# MAGIC     day INT,
# MAGIC     hour INT
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO flight_analytics.gold.DimTime AS target
# MAGIC USING (
# MAGIC     SELECT DISTINCT
# MAGIC         from_unixtime(last_contact) AS event_time,
# MAGIC         date(from_unixtime(last_contact)) AS flight_date,
# MAGIC         year(from_unixtime(last_contact)) AS year,
# MAGIC         month(from_unixtime(last_contact)) AS month,
# MAGIC         day(from_unixtime(last_contact)) AS day,
# MAGIC         hour(from_unixtime(last_contact)) AS hour
# MAGIC     FROM flight_analytics.silver.silver_table
# MAGIC ) AS source
# MAGIC ON target.event_time = source.event_time
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT (event_time, flight_date, year, month, day, hour)
# MAGIC VALUES (
# MAGIC     source.event_time,
# MAGIC     source.flight_date,
# MAGIC     source.year,
# MAGIC     source.month,
# MAGIC     source.day,
# MAGIC     source.hour
# MAGIC );
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS flight_analytics.gold.FactFlightSnapshot (
# MAGIC     DimAircraftKey BIGINT,
# MAGIC     DimTimeKey BIGINT,
# MAGIC     longitude DOUBLE,
# MAGIC     latitude DOUBLE,
# MAGIC     geo_altitude DOUBLE,
# MAGIC     velocity DOUBLE,
# MAGIC     vertical_rate DOUBLE,
# MAGIC     on_ground BOOLEAN,
# MAGIC     ingestion_time TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO flight_analytics.gold.FactFlightSnapshot
# MAGIC SELECT
# MAGIC     a.DimAircraftKey,
# MAGIC     t.DimTimeKey,
# MAGIC     s.longitude,
# MAGIC     s.latitude,
# MAGIC     s.geo_altitude,
# MAGIC     s.velocity,
# MAGIC     s.vertical_rate,
# MAGIC     s.on_ground,
# MAGIC     s.ingestion_time
# MAGIC FROM flight_analytics.silver.silver_table s
# MAGIC
# MAGIC JOIN flight_analytics.gold.DimAircraft a
# MAGIC   ON s.icao24 = a.icao24
# MAGIC
# MAGIC JOIN flight_analytics.gold.DimTime t
# MAGIC   ON from_unixtime(s.last_contact) = t.event_time
# MAGIC