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
# Delta tables. Use an explicit OneLake root: a notebook that has not had the
# Lakehouse attached resolves relative paths under the user area instead.
from pyspark.sql import functions as F

LAKEHOUSE_ROOT = (
    "abfss://fee6f10f-56a9-420a-8702-b961908c579b"
    "@onelake.dfs.fabric.microsoft.com/"
    "5aec4f99-c467-448f-8829-6a60516c2336"
)
RAW_ROOT = f"{LAKEHOUSE_ROOT}/Files/wwi-raw-data"
SILVER_TABLES = {
    "city": "silver_dim_city",
    "customer": "silver_dim_customer",
    "date": "silver_dim_date",
    "stock_item": "silver_dim_stock_item",
    "sale": "silver_fact_sale",
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

# Fabric catalogs managed tables created with saveAsTable. Writing directly to
# nested Tables/silver paths creates valid Delta data but leaves it under the
# Unidentified area, so the Silver layer is separated by table naming instead.
# Overwrite affects only these Silver tables; raw input files are never modified.
for key, frame in silver.items():
    table_name = SILVER_TABLES[key]
    (
        frame.write.mode("overwrite")
        .format("delta")
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )
    print(f"Wrote managed table {table_name}: {frame.count()} rows")


# CELL ********************

print("Silver tables:")
for table_name in SILVER_TABLES.values():
    print(table_name, spark.table(table_name).count())


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
