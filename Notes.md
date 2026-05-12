## Production on AWS:
```
# Read from S3
df = spark.read.csv("s3a://company-data/raw/orders.csv", header=True)

# Write to S3
df.write.parquet("s3a://company-data/processed/orders")
```
## Spark session configuration:
```
spark = (
    SparkSession.builder
        .appName("Orders_ETL")
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262",
        )
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )
        .getOrCreate()
)
```
## Production on GCP:
```
# Read from GCS
df = spark.read.csv("gs://company-data/raw/orders.csv", header=True)

# Write to GCS
df.write.parquet("gs://company-data/processed/orders")
```
## Spark session configuration:
```
spark = (
    SparkSession.builder
        .appName("Orders_ETL")
        .config(
            "spark.jars.packages",
            "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.11",
        )
        .getOrCreate()
)
```
## Production on Azure:
```
# Read from Azure Data Lake Storage
df = spark.read.csv(
    "abfss://container@account.dfs.core.windows.net/raw/orders.csv",
    header=True
)

# Write to ADLS
df.write.parquet(
    "abfss://container@account.dfs.core.windows.net/processed/orders"
)
```
## Spark session configuration:
```
spark = (
    SparkSession.builder
        .appName("Orders_ETL")
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-azure:3.3.4",
        )
        .getOrCreate()
)
```
## Storage Format Changes
### Parquet:
```
df.write.parquet("output/orders/")
```
### Delta Lake:
```
df.write.format("delta").save("output/orders/")
```
### Iceberg:
```
df.writeTo("company_data.orders").create()
```
## Typical Flow:
```
Source Systems → Cloud Storage → PySpark Processing → Data Warehouse → BI Tools
(APIs, DBs)      (S3, GCS, ADLS)  (EMR, Databricks)    (Snowflake, BQ)  (Tableau, Looker)
```
