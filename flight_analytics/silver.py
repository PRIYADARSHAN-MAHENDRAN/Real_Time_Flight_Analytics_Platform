# Databricks notebook source
try:
    from pyspark.sql.functions import col, explode, current_timestamp, trim
    import datetime
    print("Cell 1: Imports successful.")
except Exception as e:
    print(f"Error in Cell 1 (Imports): {e}")
    raise

# COMMAND ----------

try:
    now = datetime.datetime.now()
    bronze_path = (
        f"/Volumes/flight_analytics/bronze/sourcefiles/"
        f"year={now.year}/"
        f"month={now.month:02d}/"
        f"day={now.day:02d}"
    )
    print(f"Cell 2: Bronze path generated: {bronze_path}")
except Exception as e:
    print(f"Error in Cell 2 (Path Generation): {e}")
    raise

# COMMAND ----------

try:
    
    # Check directory exists (UC-safe)
    try:
        files = dbutils.fs.ls(bronze_path)
    except Exception:
        raise FileNotFoundError(f"Bronze directory does not exist: {bronze_path}")

    # Check at least one JSON file exists
    json_files = [f.path for f in files if f.path.endswith(".json")]
    if not json_files:
        raise ValueError(f"Bronze directory exists but contains no JSON files: {bronze_path}")

    # Read Bronze JSON (Spark supports wildcard)
    bronze_df = (
        spark.read
        .option("multiline", "true")
        .json(bronze_path + "/*.json")
    )

    # Serverless-safe empty check
    if bronze_df.limit(1).count() == 0:
        raise ValueError("Bronze JSON files are empty after read")

    print(f"Loaded {len(json_files)} Bronze JSON files successfully")

except FileNotFoundError as e:
    print("❌ FILE PATH ERROR")
    print(e)
    raise

except ValueError as e:
    print("❌ EMPTY DATA ERROR")
    print(e)
    raise

except Exception as e:
    print("❌ UNEXPECTED ERROR while reading Bronze JSON")
    print(e)
    raise

# COMMAND ----------

try:
    bronze_files_df = (
        bronze_df
        .select(col("_metadata.file_path").alias("source_file"))
        .distinct()
    )
    print("Cell 4: Extracted distinct source files from metadata.")
except Exception as e:
    print(f"Error in Cell 4 (Metadata Extraction): {e}")
    raise

# COMMAND ----------

try:
    spark.sql("""
    CREATE TABLE IF NOT EXISTS flight_analytics.silver.bronze_file_tracker (
        source_file STRING,
        processed_at TIMESTAMP
    )
    USING DELTA
    """)
    print("Cell 5: Tracker table checked/created.")
except Exception as e:
    print(f"Error in Cell 5 (Table Creation): {e}")
    raise

# COMMAND ----------

try:
    loaded_files_df = spark.table(
        "flight_analytics.silver.bronze_file_tracker"
    ).select("source_file")

    new_files_df = (
        bronze_files_df
        .join(
            loaded_files_df,
            on="source_file",
            how="left_anti"   # keep only NOT loaded
        )
    )

    if new_files_df.isEmpty():
        print("Cell 6: No new Bronze files to process. Exiting notebook.")
        dbutils.notebook.exit("No new data")
    else:
        print(f"Cell 6: Found new files to process.")
except Exception as e:
    print(f"Error in Cell 6 (Incremental Check): {e}")
    raise

# COMMAND ----------

try:
    new_bronze_df = bronze_df.join(
        new_files_df,
        bronze_df["_metadata.file_path"] == new_files_df["source_file"],
        "inner"
    )
    print("Cell 7: Filtered Bronze DataFrame for new files only.")
except Exception as e:
    print(f"Error in Cell 7 (Joining): {e}")
    raise


# COMMAND ----------

try:
    flattened_df = (
        new_bronze_df
        .select(
            col("_metadata.file_path").alias("source_file"),
            explode(col("states")).alias("state")
        )
    )
    print("Cell 8: JSON 'states' array exploded successfully.")
except Exception as e:
    print(f"Error in Cell 8 (Explode): {e}")
    raise

# COMMAND ----------

try:
    silver_df = (
        flattened_df
        .select(
            col("state")[0].alias("icao24"),
            trim(col("state")[1]).alias("callsign"),
            col("state")[2].alias("origin_country"),
            col("state")[3].cast("long").alias("time_position"),
            col("state")[4].cast("long").alias("last_contact"),
            col("state")[5].cast("double").alias("longitude"),
            col("state")[6].cast("double").alias("latitude"),
            col("state")[7].cast("double").alias("baro_altitude"),
            col("state")[8].cast("boolean").alias("on_ground"),
            col("state")[9].cast("double").alias("velocity"),
            col("state")[10].cast("double").alias("heading"),
            col("state")[11].cast("double").alias("vertical_rate"),
            col("state")[12].cast("string").alias("sensors"),
            col("state")[13].cast("double").alias("geo_altitude"),
            col("state")[14].alias("squawk"),
            col("state")[15].cast("boolean").alias("spi"),
            col("state")[16].cast("int").alias("position_source"),
            current_timestamp().alias("ingestion_time"),
            col("source_file")
        )
    )
    print("Cell 9: Data schema transformation and casting complete.")
except Exception as e:
    print(f"Error in Cell 9 (Transformation): {e}")
    raise

# COMMAND ----------

try:
    silver_df = silver_df.filter(
        col("icao24").isNotNull() &
        col("longitude").isNotNull() &
        col("latitude").isNotNull()
    )
    print("Cell 10: Null filters applied to icao24 and coordinates.")
except Exception as e:
    print(f"Error in Cell 10 (Filtering): {e}")
    raise

# COMMAND ----------

try:
    # Ensure Silver Table exists
    spark.sql("""
    CREATE TABLE IF NOT EXISTS flight_analytics.silver.silver_table (
        icao24 STRING, callsign STRING, origin_country STRING, 
        time_position LONG, last_contact LONG, longitude DOUBLE, 
        latitude DOUBLE, baro_altitude DOUBLE, on_ground BOOLEAN, 
        velocity DOUBLE, heading DOUBLE, vertical_rate DOUBLE, 
        sensors STRING, geo_altitude DOUBLE, squawk STRING, 
        spi BOOLEAN, position_source INT, ingestion_time TIMESTAMP, 
        source_file STRING
    )
    USING DELTA;
    """)
    
    silver_df.write \
        .mode("append") \
        .format("delta") \
        .saveAsTable("flight_analytics.silver.silver_table")
    print("Cell 11: Silver table updated successfully.")
except Exception as e:
    print(f"Error in Cell 11 (Silver Write): {e}")
    raise

# COMMAND ----------

try:
    tracker_df = (
        new_files_df
        .withColumn("processed_at", current_timestamp())
    )

    tracker_df.write \
        .mode("append") \
        .format("delta") \
        .saveAsTable("flight_analytics.silver.bronze_file_tracker")
    print("Cell 12: Tracker table updated with new file logs.")
except Exception as e:
    print(f"Error in Cell 12 (Tracker Write): {e}")
    raise

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from flight_analytics.silver.silver_table