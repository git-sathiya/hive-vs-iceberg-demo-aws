import os
import subprocess
import json
from pyspark.sql import SparkSession

# Try to load AWS credentials dynamically from the profile if it is an SSO profile
try:
    cmd = ["aws", "configure", "export-credentials", "--profile", "iceberg-demo"]
    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    creds = json.loads(result.stdout)
    os.environ["AWS_ACCESS_KEY_ID"] = creds["AccessKeyId"]
    os.environ["AWS_SECRET_ACCESS_KEY"] = creds["SecretAccessKey"]
    os.environ["AWS_SESSION_TOKEN"] = creds["SessionToken"]
    print("Successfully exported AWS SSO credentials from profile 'iceberg-demo'.")
except Exception as e:
    print(f"Warning: Could not export credentials for profile 'iceberg-demo': {e}")
    os.environ["AWS_PROFILE"] = "iceberg-demo"

# --- FINAL CONFIGURATION ---
S3_BUCKET = "iceberg-vs-hive-demo-mumbai"
HIVE_DATABASE = "hive_demo_db"
AWS_REGION = "ap-south-1"
os.environ["AWS_REGION"] = AWS_REGION

HIVE_PATH = f"s3a://{S3_BUCKET}/hive-warehouse/"

# Spark needs the standard Hadoop-AWS connector for legacy Hive tables on S3
os.environ['PYSPARK_SUBMIT_ARGS'] = "--packages org.apache.hadoop:hadoop-aws:3.3.4 pyspark-shell"

print("Initializing PySpark Session for Classic Hive Table metadata...")

spark = SparkSession.builder \
    .appName("AWS-Hive-Comparison-Demo") \
    .config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.EnvironmentVariableCredentialsProvider") \
    .config("spark.hadoop.fs.s3a.endpoint.region", AWS_REGION) \
    .enableHiveSupport() \
    .getOrCreate()

target_hive_table = f"{HIVE_DATABASE}.hive_sales_demo"

# 1. Cleanup and Build External Hive Table
spark.sql(f"CREATE DATABASE IF NOT EXISTS {HIVE_DATABASE}")
spark.sql(f"DROP TABLE IF EXISTS {target_hive_table}")
print(f"Creating Traditional Hive Table pointing to S3: {target_hive_table}...")
spark.sql(f"""
    CREATE EXTERNAL TABLE {target_hive_table} (
        sale_id STRING,
        customer_id STRING,
        amount DOUBLE,
        category STRING
    )
    PARTITIONED BY (sale_date STRING)
    STORED AS PARQUET
    LOCATION '{HIVE_PATH}'
""")

# 2. Add Data via legacy insert syntax
print("Adding data to Hive table...")
spark.sql(f"""
    INSERT INTO {target_hive_table} PARTITION (sale_date='2026-07-01') 
    VALUES ('101', 'CUST_A', 250.50, 'Electronics')
""")

# 3. ACID Transaction Failure Simulation 
print("\nAttempting Row-level UPDATE on classic Hive table (Should fail)...")
try:
    spark.sql(f"UPDATE {target_hive_table} SET amount = 275.00 WHERE sale_id = '101'")
except Exception as e:
    print(f"\n❌ EXPECTED HIVE LIMITATION CAUGHT SUCCESSFULLY:\n{e}")

spark.stop()