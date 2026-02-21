from pyspark.sql import SparkSession
import pyspark
import os
import json
import time
from urllib import request, error

def ensure_catalog_exists(uc_uri: str, catalog_name: str) -> None:
    base = uc_uri.rstrip("/")
    catalogs_url = f"{base}/api/2.1/unity-catalog/catalogs"

    with request.urlopen(catalogs_url) as resp:
        data = json.loads(resp.read().decode("utf-8"))
    existing = {c.get("name") for c in data.get("catalogs", [])}
    if catalog_name in existing:
        print(f"Catalog '{catalog_name}' already exists in Unity Catalog.")
        return

    payload = json.dumps({
        "name": catalog_name,
        "comment": "Auto-created by scripts/verify_setup.py",
    }).encode("utf-8")
    req = request.Request(
        catalogs_url,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with request.urlopen(req) as resp:
        created = json.loads(resp.read().decode("utf-8"))
    print(f"Created catalog '{created.get('name', catalog_name)}' in Unity Catalog.")

def verify_unity_catalog():
    print("Starting Unity Catalog Verification...")
    spark_master = os.getenv("SPARK_MASTER", "local[*]")
    uc_uri = os.getenv("UC_URI", "http://localhost:8081")
    spark_catalog_name = os.getenv("SPARK_CATALOG_NAME", "spark_catalog")
    schema_name = os.getenv("SPARK_SCHEMA_NAME", "default_schema")
    table_name = os.getenv("SPARK_TEST_TABLE", f"test_table_{int(time.time())}")
    spark_shared_location = os.getenv("SPARK_SHARED_LOCATION", "")
    scala_binary = os.getenv("SCALA_BINARY_VERSION", "2.12")
    uc_version = os.getenv("UC_SPARK_VERSION", "0.2.1")
    delta_version = os.getenv("DELTA_SPARK_VERSION", "3.2.1")
    scala_library_version = os.getenv("SCALA_LIBRARY_VERSION", "2.12.18")
    spark_packages = ",".join([
        f"io.unitycatalog:unitycatalog-spark_{scala_binary}:{uc_version}",
        f"io.delta:delta-spark_{scala_binary}:{delta_version}",
        f"org.scala-lang:scala-library:{scala_library_version}",
    ])
    print(f"Using spark.jars.packages={spark_packages}")
    print(f"Using pyspark={pyspark.__version__}")

    expected_spark_version = os.getenv("EXPECTED_SPARK_VERSION", "3.5.3")
    if pyspark.__version__ != expected_spark_version:
        raise RuntimeError(
            "PySpark version mismatch: "
            f"expected {expected_spark_version} but found {pyspark.__version__}. "
            "Install matching deps with: pip install -r scripts/requirements.txt"
        )
    try:
        ensure_catalog_exists(uc_uri, spark_catalog_name)
    except error.URLError as exc:
        raise RuntimeError(f"Unable to reach Unity Catalog API at {uc_uri}: {exc}") from exc

    builder = SparkSession.builder \
        .appName("UnityCatalogVerification") \
        .master(spark_master) \
        .config("spark.jars.packages", spark_packages) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "io.unitycatalog.spark.UCSingleCatalog") \
        .config("spark.sql.catalog.spark_catalog.uri", uc_uri) \
        .config("spark.sql.catalog.spark_catalog.token", "") \
        .config("spark.sql.defaultCatalog", spark_catalog_name)

    spark = builder.getOrCreate()

    print("Spark Session Created.")
    
    # Debug: List Catalogs
    print("Listing Catalogs:")
    spark.sql("SHOW CATALOGS").show()
    
    try:
        print(f"Attempting 'USE {spark_catalog_name}.default'...")
        spark.sql(f"USE {spark_catalog_name}.default")
        print(f"Switched to catalog '{spark_catalog_name}'")
    except Exception as e:
        print(f"Failed to switch to catalog using USE: {e}")

    try:
        print(f"Creating schema '{schema_name}'...")
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {spark_catalog_name}.{schema_name}")

        print(f"Listing Schemas in {spark_catalog_name}:")
        spark.sql(f"SHOW SCHEMAS IN {spark_catalog_name}").show()
        spark.sql(f"USE {spark_catalog_name}.{schema_name}")
        print(f"Using namespace {spark_catalog_name}.{schema_name}")

        if spark_master.startswith("spark://"):
            if not spark_shared_location:
                raise RuntimeError(
                    "SPARK_MASTER uses standalone cluster mode. Set SPARK_SHARED_LOCATION to a "
                    "path shared by driver and executors (for example a bind-mounted volume), "
                    "or run with SPARK_MASTER=local[*]."
                )
            temp_dir = f"{spark_shared_location.rstrip('/')}/uc_verify_{int(time.time())}"
        else:
            import tempfile
            temp_dir = tempfile.mkdtemp()

        full_table_name = f"{spark_catalog_name}.{schema_name}.{table_name}"
        print(f"Creating table '{full_table_name}' at {temp_dir}...")
        spark.sql(f"CREATE TABLE {full_table_name} (id INT, data STRING) USING DELTA LOCATION '{temp_dir}'")

        print("Inserting data...")
        spark.sql(f"INSERT INTO {full_table_name} VALUES (1, 'test_data'), (2, 'more_data')")

        print("Reading data back...")
        df = spark.sql(f"SELECT * FROM {full_table_name}")
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
