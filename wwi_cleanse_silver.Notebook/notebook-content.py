# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "5aec4f99-c467-448f-8829-6a60516c2336",
# META       "default_lakehouse_name": "wwi",
# META       "default_lakehouse_workspace_id": "fee6f10f-56a9-420a-8702-b961908c579b",
# META       "known_lakehouses": [
# META         {
# META           "id": "5aec4f99-c467-448f-8829-6a60516c2336"
# META         }
# META       ]
# META     }
# META   }
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
TABLE_NAMESPACE = "wwi"
SILVER_TABLES = {
    "city": "silver_dim_city",
    "customer": "silver_dim_customer",
    "date": "silver_dim_date",
    "stock_item": "silver_dim_stock_item",
    "sale": "silver_fact_sale",
}
GOLD_TABLES = {
    "sales_by_year": "gold_sales_by_year",
    "sales_by_city": "gold_sales_by_city",
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


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

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


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Conservative, schema-preserving cleansing: trim text, remove exact duplicate
# rows, and reject rows that are entirely null.
silver = {
    key: clean_strings(frame).dropDuplicates().na.drop(how="all")
    for key, frame in raw.items()
}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Fabric catalogs managed tables created with saveAsTable. Writing directly to
# nested Tables/silver paths creates valid Delta data but leaves it under the
# Unidentified area, so the Silver layer is separated by table naming instead.
# Overwrite affects only these Silver tables; raw input files are never modified.
for key, frame in silver.items():
    table_name = SILVER_TABLES[key]
    qualified_table_name = f"{TABLE_NAMESPACE}.{table_name}"
    (
        frame.write.mode("overwrite")
        .format("delta")
        .option("overwriteSchema", "true")
        .saveAsTable(qualified_table_name)
    )
    print(f"Wrote managed table {qualified_table_name}: {frame.count()} rows")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("Silver tables:")
for table_name in SILVER_TABLES.values():
    qualified_table_name = f"{TABLE_NAMESPACE}.{table_name}"
    print(qualified_table_name, spark.table(qualified_table_name).count())


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Gold is a small presentation layer built only from managed Silver tables.
# It deliberately leaves both the raw Files and Silver tables untouched.
sales = spark.table(f"{TABLE_NAMESPACE}.{SILVER_TABLES['sale']}")
cities = spark.table(f"{TABLE_NAMESPACE}.{SILVER_TABLES['city']}")

gold = {
    "sales_by_year": (
        sales.withColumn("CalendarYear", F.year("InvoiceDateKey"))
        .groupBy("CalendarYear")
        .agg(
            F.countDistinct("WWIInvoiceID").alias("InvoiceCount"),
            F.sum("Quantity").alias("QuantitySold"),
            F.sum("TotalExcludingTax").alias("SalesExcludingTax"),
            F.sum("TaxAmount").alias("TaxAmount"),
            F.sum("Profit").alias("Profit"),
            F.sum("TotalIncludingTax").alias("SalesIncludingTax"),
        )
        .orderBy("CalendarYear")
    ),
    "sales_by_city": (
        sales.join(cities.select("CityKey", "City", "Country"), "CityKey", "left")
        .groupBy("CityKey", "City", "Country")
        .agg(
            F.countDistinct("WWIInvoiceID").alias("InvoiceCount"),
            F.sum("Quantity").alias("QuantitySold"),
            F.sum("TotalIncludingTax").alias("SalesIncludingTax"),
            F.sum("Profit").alias("Profit"),
        )
        .orderBy(F.col("SalesIncludingTax").desc())
    ),
}

for key, frame in gold.items():
    table_name = f"{TABLE_NAMESPACE}.{GOLD_TABLES[key]}"
    (
        frame.write.mode("overwrite")
        .format("delta")
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )
    print(f"Wrote managed table {table_name}: {frame.count()} rows")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("Gold tables:")
for table_name in GOLD_TABLES.values():
    qualified_table_name = f"{TABLE_NAMESPACE}.{table_name}"
    print(qualified_table_name, spark.table(qualified_table_name).count())


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
