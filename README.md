# Spark + Unity Catalog (Docker)

This project runs:
- Unity Catalog server on `http://localhost:8081`
- Unity Catalog UI on `http://localhost:3000`
- Spark standalone master on `spark://localhost:7077` (UI `http://localhost:8082`)

## Why this configuration

Unity Catalog Spark integration in this workspace uses Scala `2.12` artifacts:
- `io.unitycatalog:unitycatalog-spark_2.12:0.2.1`
- `io.delta:delta-spark_2.12:3.2.1`

So Spark must also be Scala `2.12`. The compose file uses Spark `3.5.3` with Scala `2.12`.

## Start services

```bash
docker compose up -d
docker compose ps
```

## Verify Unity Catalog is reachable

```bash
curl http://localhost:8081
```

## Verify from Spark (inside container)

```bash
docker compose exec spark-master /opt/spark/bin/spark-sql -e "SHOW CATALOGS"
docker compose exec spark-master /opt/spark/bin/spark-sql -e "CREATE SCHEMA IF NOT EXISTS spark_catalog.default_schema"
docker compose exec spark-master /opt/spark/bin/spark-sql -e "CREATE TABLE IF NOT EXISTS spark_catalog.default_schema.test_table (id INT, data STRING) USING DELTA LOCATION '/tmp/test_table_delta'"
docker compose exec spark-master /opt/spark/bin/spark-sql -e "INSERT INTO spark_catalog.default_schema.test_table VALUES (1, 'a'), (2, 'b')"
docker compose exec spark-master /opt/spark/bin/spark-sql -e "SELECT * FROM spark_catalog.default_schema.test_table"
```

## Optional Python verification from host

`scripts/verify_setup.py` can run against:
- local Spark: default (`SPARK_MASTER=local[*]`)
- standalone Spark: `SPARK_MASTER=spark://localhost:7077`

Important: the Python environment must use `pyspark==3.5.3` to match the Docker Spark master version.

Example:

```bash
UC_URI=http://localhost:8081 SPARK_MASTER=local[*] python scripts/verify_setup.py
```
