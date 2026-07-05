import os
import random
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# ==========================================
# 1. CONFIGURE SPARK WITH AWS GLUE & ICEBERG
# ==========================================

# TODO: Replace with your actual S3 bucket name
S3_BUCKET = "s3://your-aws-demo-bucket-name/iceberg-warehouse/"

print("Initializing Spark Session with AWS Glue Catalog and Apache Iceberg...")

spark = SparkSession.builder \
    .appName("AWS Native Iceberg Demo") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.aws_glue", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.aws_glue.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config("spark.sql.catalog.aws_glue.warehouse", S3_BUCKET) \
    .getOrCreate()

print(f"Spark Version: {spark.version}")
print(f"Iceberg Catalog Configured: aws_glue")
print(f"S3 Warehouse Location: {S3_BUCKET}")


# ==========================================
# STEP 1: CREATE TABLE WITH EXPRESSION PARTITIONING
# ==========================================
print("\n--- Step 1: Creating Database and Table with Expression-Based Partitioning ---")

# Create database in AWS Glue Catalog
spark.sql("CREATE DATABASE IF NOT EXISTS aws_glue.iceberg_demo")

# Create table with NATIVE ICEBERG expression-based partitioning
spark.sql("""
CREATE OR REPLACE TABLE aws_glue.iceberg_demo.employee_demographics (
    emp_id INT,
    name STRING,
    dept_name STRING,
    salary INT,
    join_date DATE,
    performance_score STRING
)
USING iceberg
PARTITIONED BY (months(join_date))
TBLPROPERTIES ('format-version' = '2')
""")

# Generate and Append Test Data (10,000 records)
print("Generating 10,000 mock records...")
schema = StructType([
    StructField("emp_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("dept_name", StringType(), True),
    StructField("salary", IntegerType(), True),
    StructField("join_date", DateType(), True),
    StructField("performance_score", StringType(), True)
])

first_names = ["John", "Jane", "Michael", "Emily", "David", "Sarah", "James", "Jessica", "Robert", "Karen"]
last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Miller", "Davis", "Garcia", "Rodriguez", "Wilson"]
departments = ["Engineering", "Marketing", "Operations", "Finance", "HR", "Sales"]
scores = ["Excellent", "Good", "Average", "Needs Improvement"]

base_id = 100
start_date = datetime.strptime("2026-01-01", "%Y-%m-%d")

for batch_num in range(1, 6):
    batch_data = []
    for i in range(2000):
        emp_id = base_id + (batch_num * 2000) + i
        name = f"{random.choice(first_names)} {random.choice(last_names)}"
        dept = random.choice(departments)
        salary = random.randint(60000, 160000)
        random_days = random.randint(0, 150)
        join_date = (start_date + timedelta(days=random_days)).date()
        score = random.choice(scores)
        batch_data.append((emp_id, name, dept, salary, join_date, score))
    
    batch_df = spark.createDataFrame(batch_data, schema=schema)
    batch_df.writeTo("aws_glue.iceberg_demo.employee_demographics").append()
    
print("Successfully loaded 10,000 records into S3!")


# ==========================================
# STEP 2: QUERY METADATA TABLES
# ==========================================
print("\n--- Step 2: Querying Native Iceberg Metadata Tables ---")

print("\n[Partitions Metadata]")
spark.sql("SELECT partition, record_count, file_count, spec_id FROM aws_glue.iceberg_demo.employee_demographics.partitions ORDER BY partition").show(truncate=False)

print("\n[Files Metadata (Truncated to 5)]")
spark.sql("SELECT file_path, partition, record_count, file_size_in_bytes, file_format FROM aws_glue.iceberg_demo.employee_demographics.files LIMIT 5").show(truncate=False)

print("\n[Snapshots Metadata]")
spark.sql("SELECT committed_at, snapshot_id, parent_id, operation FROM aws_glue.iceberg_demo.employee_demographics.snapshots ORDER BY committed_at DESC").show(truncate=False)


# ==========================================
# STEP 3: SCHEMA EVOLUTION & ROW-LEVEL OPS
# ==========================================
print("\n--- Step 3: Running Schema Evolution and Updates ---")

# Row-level update
spark.sql("UPDATE aws_glue.iceberg_demo.employee_demographics SET salary = 175000 WHERE emp_id = 2001")

# Add new columns
spark.sql("ALTER TABLE aws_glue.iceberg_demo.employee_demographics ADD COLUMNS (is_active BOOLEAN, department_code STRING)")

# Rename column
spark.sql("ALTER TABLE aws_glue.iceberg_demo.employee_demographics RENAME COLUMN dept_name TO department")

# Insert data matching the newly evolved schema
spark.sql("""
INSERT INTO aws_glue.iceberg_demo.employee_demographics VALUES 
(99999, 'Tony Stark', 'Engineering', 250000, CAST('2026-06-01' AS DATE), 'Excellent', true, 'ENG-01')
""")

print("\nVerifying updated and new records:")
spark.sql("SELECT emp_id, name, department, salary, is_active FROM aws_glue.iceberg_demo.employee_demographics WHERE emp_id IN (2001, 99999)").show()


# ==========================================
# STEP 4: TIME TRAVEL
# ==========================================
print("\n--- Step 4: Time Travel with Snapshot IDs ---")

# Safely extract historical snapshot ID
snapshots_df = spark.sql("SELECT snapshot_id FROM aws_glue.iceberg_demo.employee_demographics.snapshots ORDER BY committed_at LIMIT 1")

if snapshots_df.count() > 0:
    snapshot_id = snapshots_df.collect()[0]['snapshot_id']
    print(f"Traveling back in time to Snapshot ID: {snapshot_id}")
    
    historical_df = spark.read \
        .option("snapshot-id", str(snapshot_id)) \
        .table("aws_glue.iceberg_demo.employee_demographics")
    
    print(f"Historical record count (Before schema evolution/updates): {historical_df.count()}")
else:
    print("No historical snapshots found.")


# ==========================================
# STEP 5: PARTITION EVOLUTION
# ==========================================
print("\n--- Step 5: Native Iceberg Partition Evolution ---")

# Drop the monthly partition rule, change it to daily
spark.sql("ALTER TABLE aws_glue.iceberg_demo.employee_demographics DROP PARTITION FIELD months(join_date)")
spark.sql("ALTER TABLE aws_glue.iceberg_demo.employee_demographics ADD PARTITION FIELD days(join_date)")

# Insert data under the new partition layout spec
spark.sql("""
INSERT INTO aws_glue.iceberg_demo.employee_demographics VALUES 
(88888, 'Bruce Banner', 'Research', 180000, CAST('2026-06-15' AS DATE), 'Good', true, 'RES-01')
""")

print("\nVerifying Partition Evolution specs metadata:")
spark.sql("SELECT spec_id, partition, file_count, record_count FROM aws_glue.iceberg_demo.employee_demographics.partitions ORDER BY spec_id, partition").show(truncate=False)


# ==========================================
# STEP 6: ICEBERG MAINTENANCE PROCEDURES
# ==========================================
print("\n--- Step 6: Native Iceberg System Procedures (Compaction) ---")

# Run file compaction
spark.sql("""
CALL aws_glue.system.rewrite_data_files(
  table => 'iceberg_demo.employee_demographics',
  strategy => 'sort',
  sort_order => 'emp_id ASC'
)
""").show(truncate=False)

print("\nDemo script execution completed successfully on AWS!")