# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# This notebook turns the copied WWI parquet files into reproducible Silver
# Delta tables. Keep the path relative to the attached Lakehouse so the same
# source works in Fabric and in a Livy session.
from pyspark.sql import functions as F

RAW_ROOT = "Files/wwi-raw-data"
SILVER_ROOT = "Tables/silver"
SILVER_TABLES = {
    "city": "dim_city",
    "customer": "dim_customer",
    "date": "dim_date",
    "stock_item": "dim_stock_item",
    "sale": "fact_sale",
}


def read_raw(name):
    """Read one of the parquet files produced by the WWI copy job."""
    return spark.read.parquet(f"{RAW_ROOT}/{name}.parquet")


def clean_strings(frame):
    """Trim string columns while preserving the source schema otherwise."""
    return frame.select(
        *[
            F.trim(F.col(column)).alias(column)
            if data_type == "string"
            else F.col(column)
            for column, data_type in frame.dtypes
        ]
    )


# CELL ********************

# Load all five entities written by the WWI copy job. Explicit names make a
# missing input fail early instead of silently producing incomplete Silver data.
raw = {
    key: read_raw(filename)
    for key, filename in {
        "city": "DimCity",
        "customer": "DimCustomer",
        "date": "DimDate",
        "stock_item": "DimStockItem",
        "sale": "FactSale",
    }.items()
}
print("Loaded raw row counts:", {key: frame.count() for key, frame in raw.items()})


# CELL ********************

# Conservative, schema-preserving cleansing: trim text, remove exact duplicate
# rows, and reject rows that are entirely null.
silver = {
    key: clean_strings(frame).dropDuplicates().na.drop(how="all")
    for key, frame in raw.items()
}


# CELL ********************

# Overwrite is intentional only for this notebook's own Tables/silver paths:
# the source Files/wwi-raw-data files are never modified.
for key, frame in silver.items():
    table_name = SILVER_TABLES[key]
    silver_path = f"{SILVER_ROOT}/{table_name}"
    (
        frame.write.mode("overwrite")
        .format("delta")
        .option("overwriteSchema", "true")
        .save(silver_path)
    )
    print(f"Wrote {silver_path}: {frame.count()} rows")


# CELL ********************

print("Silver tables:")
for table_name in SILVER_TABLES.values():
    silver_path = f"{SILVER_ROOT}/{table_name}"
    print(silver_path, spark.read.format("delta").load(silver_path).count())


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
