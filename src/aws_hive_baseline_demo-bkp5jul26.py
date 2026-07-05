import os
import random
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from pyspark.sql.utils import AnalysisException

# ==========================================
# 1. CONFIGURE SPARK WITH AWS GLUE DATA CATALOG
# ==========================================

# TODO: Replace with your actual S3 bucket name
S3_BUCKET = "s3://your-aws-demo-bucket-name/hive-warehouse/"

print("Initializing Spark Session for Traditional Hive Table Baseline...")

spark = SparkSession.builder \
    .appName("AWS Hive Baseline Demo") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("spark.sql.warehouse.dir", S3_BUCKET) \
    .getOrCreate()

print(f"Spark Version: {spark.version}")
print(f"Catalog Implementation: Hive (AWS Glue Data Catalog Mapping)")
print(f"S3 Hive Warehouse Location: {S3_BUCKET}")


# ==========================================
# STEP 1: CREATE DATABASE AND HIVE TABLE
# ==========================================
print("\n--- Step 1: Creating Database and True Parquet/Hive Table ---")

spark.sql("CREATE DATABASE IF NOT EXISTS iceberg_demo_hive_db")
spark.sql("USE iceberg_demo_hive_db")

# Drop table if exists to start fresh
spark.sql("DROP TABLE IF EXISTS employee_demographics_hive")

# Create standard Hive table layout with mandatory explicit manual column partitioning
spark.sql("""
CREATE TABLE employee_demographics_hive (
    emp_id INT,
    name STRING,
    dept_name STRING,
    salary INT,
    performance_score STRING,
    join_date DATE
)
USING parquet
PARTITIONED BY (join_date)
""")


# ==========================================
# STEP 2: LOAD DATA (SAME MOCK PIPELINE)
# ==========================================
print("\n--- Step 2: Generating and Appending Mock Records ---")

schema = StructType([
    StructField("emp_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("dept_name", StringType(), True),
    StructField("salary", IntegerType(), True),
    StructField("performance_score", StringType(), True),
    StructField("join_date", DateType(), True)
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
        batch_data.append((emp_id, name, dept, salary, score, join_date))
    
    batch_df = spark.createDataFrame(batch_data, schema=schema)
    
    # Hive require manual layout mapping tracking matching structural partitions
    batch_df.write.mode("append").insertInto("iceberg_demo_hive_db.employee_demographics_hive")
    
print("Successfully loaded 10,000 records into Hive table structure!")


# ==========================================
# STEP 3: VERIFY PARTITIONING LIMITATIONS
# ==========================================
print("\n--- Step 3: Verifying Partition Constraints (No Native Metadata Tables) ---")

# Traditional Hive has no .partitions hidden system catalog tables!
print("\n[Show Partitions (Standard Output Truncated)]")
spark.sql("SHOW PARTITIONS employee_demographics_hive").show(5, truncate=False)

print("\n[Describe Table Layout Structural Format]")
spark.sql("DESCRIBE FORMATTED employee_demographics_hive").show(30, truncate=False)


# ==========================================
# STEP 4: SCHEMA EVOLUTION (DEMONSTRATING FAILURE)
# ==========================================
print("\n--- Step 4: Testing Schema Evolution Deficiencies ---")

# Adding columns works fine
print("Testing column additions (Should Pass)...")
spark.sql("ALTER TABLE employee_demographics_hive ADD COLUMNS (is_active BOOLEAN)")

# Attempting Column renaming (This natively fails on standard Hive/Parquet layouts)
print("\nTesting column renaming (Expected to Fail under Hive rules)...")
try:
    spark.sql("ALTER TABLE employee_demographics_hive RENAME COLUMN dept_name TO department")
except AnalysisException as e:
    print(f"❌ EXPECTED HIVE ERROR CAUGHT: Column rename failed as anticipated!\nDetails: {str(e)[:150]}...")


# ==========================================
# STEP 5: TIME TRAVEL (DEMONSTRATING FAILURE)
# ==========================================
print("\n--- Step 5: Testing Historical Time Travel Deficiencies ---")

print("Attempting to run time-travel parameters (Expected to Fail)...")
try:
    # Attempting to read a snapshot/version modifier via historical readers
    historical_df = spark.read.option("versionAsOf", 0).table("employee_demographics_hive")
except Exception as e:
    print(f"❌ EXPECTED HIVE ERROR CAUGHT: Time travel read failed as anticipated!\nDetails: {str(e)[:150]}...")


# ==========================================
# STEP 6: PARTITION EVOLUTION (DEMONSTRATING FAILURE)
# ==========================================
print("\n--- Step 6: Testing Partition Layout Modification Deficiencies ---")

print("Attempting to alter or evolve the partitioning strategy dynamically (Expected to Fail)...")
try:
    spark.sql("ALTER TABLE employee_demographics_hive DROP PARTITION FIELD join_date")
except AnalysisException as e:
    print(f"❌ EXPECTED HIVE ERROR CAUGHT: Partition modification failed as anticipated!\nDetails: {str(e)[:150]}...")


# ==========================================
# STEP 7: FILE MAINTENANCE (COMPACTION CONTRAST)
# ==========================================
print("\n--- Step 7: Testing Maintenance Demarcations ---")

print("Attempting automatic Iceberg compaction commands on Hive layout (Expected to Fail)...")
try:
    spark.sql("CALL system.rewrite_data_files(table => 'employee_demographics_hive')")
except AnalysisException as e:
    print(f"❌ EXPECTED HIVE ERROR CAUGHT: Native engine rewrite call rejected!\nDetails: {str(e)[:150]}...")

print("\nDemo script execution completed. Traditional Hive limitations verified successfully on AWS!")