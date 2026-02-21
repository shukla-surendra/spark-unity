from pyspark.sql import SparkSession
import os
import time

def verify_unity_catalog():
    print("Starting Unity Catalog Verification...")

    # Configure Spark Session
    # Note: When running from host, we might need to point to localhost if ports are exposed
    # But for simplicity, we will assume we can reach the master via localhost:7077
    # and UC via localhost:8081 (mapped to 8080)
    
    # However, since the spark-master container needs to talk to UC container, 
    # and if we submit a job to spark-master, the worker nodes need to talk to UC.
    # The worker nodes are in the docker network, so they can resolve 'unity-catalog'.
    
    # If we run this script in 'client' mode from the host, the driver runs on the host.
    # The driver needs to talk to UC. 
    # Host -> localhost:8081 -> UC:8080.
    
    builder = SparkSession.builder \
        .appName("UnityCatalogVerification") \
        .master("local[*]") \
        .config("spark.jars.packages", "io.unitycatalog:unitycatalog-spark_2.12:0.2.1,io.delta:delta-spark_2.12:3.2.1") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.catalog.unity", "io.unitycatalog.spark.UCSingleCatalog") \
        .config("spark.sql.catalog.unity.uri", "http://localhost:8081") \
        .config("spark.sql.catalog.unity.token", "") \
        .config("spark.sql.defaultCatalog", "unity")

    spark = builder.getOrCreate()

    print("Spark Session Created.")
    
    # Debug: List Catalogs
    print("Listing Catalogs:")
    spark.sql("SHOW CATALOGS").show()
    
    # Debug: Try to use catalog
    try:
        # Try USE unity (without CATALOG keyword if that failed, or maybe USE unity.default)
        # Spark SQL 'USE' sets database. 'USE CATALOG' sets catalog.
        # If 'USE CATALOG' failed, maybe 'unity' is treated as database?
        # But SHOW CATALOGS showed it as catalog.
        
        # Let's try setting current catalog via property if USE fails, 
        # but technically we are already qualifying names.
        
        print("Attempting 'USE unity'...")
        spark.sql("USE unity")
        print("Switched to catalog 'unity'")
    except Exception as e:
        print(f"Failed to switch to catalog 'unity' using USE: {e}")

    try:
        # 1. Create a Schema (if not exists)
        print("Creating schema 'default_schema'...")
        spark.sql("CREATE SCHEMA IF NOT EXISTS unity.default_schema")
        
        print("Listing Schemas in unity:")
        spark.sql("SHOW SCHEMAS IN unity").show()
        
        # 2. Create a Table
        # Try defining a location to avoid managed table issues if that is the cause
        import tempfile
        temp_dir = tempfile.mkdtemp()
        print(f"Creating table 'unity.default_schema.test_table' at {temp_dir}...")
        
        spark.sql(f"CREATE TABLE IF NOT EXISTS unity.default_schema.test_table (id INT, data STRING) USING DELTA LOCATION '{temp_dir}'")
        
        # 3. Insert Data
        print("Inserting data...")
        spark.sql("INSERT INTO unity.default_schema.test_table VALUES (1, 'test_data'), (2, 'more_data')")
        
        # 4. Read Data
        print("Reading data back...")
        df = spark.sql("SELECT * FROM unity.default_schema.test_table")
        df.show()
        
        count = df.count()
        print(f"Row count: {count}")
        
        if count >= 2:
            print("VERIFICATION SUCCESS: Data written and read from Unity Catalog table.")
        else:
            print("VERIFICATION FAILED: Row count mismatch.")

    except Exception as e:
        print(f"VERIFICATION FAILED with error: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    verify_unity_catalog()
