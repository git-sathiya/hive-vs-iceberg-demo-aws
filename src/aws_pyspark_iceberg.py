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
ICEBERG_DATABASE = "iceberg_demo_db"
AWS_REGION = "ap-south-1"
os.environ["AWS_REGION"] = AWS_REGION

# Define Warehouse Path
WAREHOUSE_PATH = f"s3a://{S3_BUCKET}/iceberg-warehouse/"

# Inject required Maven packages for Iceberg + AWS Glue Catalog bundles
os.environ['PYSPARK_SUBMIT_ARGS'] = (
    "--packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,"
    "software.amazon.awssdk:bundle:2.20.160,"
    "org.apache.hadoop:hadoop-aws:3.3.4 pyspark-shell"
)

print("Initializing PySpark Session with AWS Glue Catalog integration...")

# Initialize Spark Session with target parameters
spark = SparkSession.builder \
    .appName("AWS-Iceberg-Features-Demo") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config("spark.sql.catalog.glue_catalog.warehouse", WAREHOUSE_PATH) \
    .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.EnvironmentVariableCredentialsProvider") \
    .config("spark.hadoop.fs.s3a.endpoint.region", AWS_REGION) \
    .getOrCreate()

print("Spark Session successfully created.\n")

# --- DEMO WORKFLOW ---
target_table = f"glue_catalog.{ICEBERG_DATABASE}.iceberg_sales_demo"

# 1. Cleanup and Build Table
spark.sql(f"CREATE DATABASE IF NOT EXISTS glue_catalog.{ICEBERG_DATABASE}")
spark.sql(f"DROP TABLE IF EXISTS {target_table}")
print(f"Creating Native Apache Iceberg Table: {target_table}...")
spark.sql(f"""
    CREATE TABLE {target_table} (
        sale_id STRING,
        customer_id STRING,
        amount DOUBLE,
        sale_date DATE,
        category STRING
    )
    USING iceberg
    TBLPROPERTIES ('format-version'='2')
""")

# 2. Insert Initial Baseline Data
print("Inserting initial mock rows...")
spark.sql(f"""
    INSERT INTO {target_table} VALUES 
    ('101', 'CUST_A', 250.50, DATE '2026-07-01', 'Electronics'),
    ('102', 'CUST_B', 15.00, DATE '2026-07-02', 'Books'),
    ('103', 'CUST_C', 99.00, DATE '2026-07-05', 'Electronics')
""")

# 3. Row-Level Update Test (ACID Feature Demonstration)
print("Executing row-level UPDATE...")
spark.sql(f"UPDATE {target_table} SET amount = 275.00 WHERE sale_id = '101'")

# 4. Querying Hidden Iceberg Snapshots Metadata (Time Travel Prep)
print("\nInspecting Table Snapshots Metadata:")
spark.sql(f"SELECT snapshot_id, committed_at, operation FROM {target_table}.snapshots").show(truncate=False)

print("🚀 Iceberg PySpark Workflow completed successfully.")
spark.stop()