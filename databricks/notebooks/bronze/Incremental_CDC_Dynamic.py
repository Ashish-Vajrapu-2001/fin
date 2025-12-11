# Incremental_CDC_Dynamic.py
import pyodbc
import json
from pyspark.sql.functions import current_timestamp, lit, col
from delta.tables import *

# 1. Inputs (Same as Initial)
dbutils.widgets.text("table_id", "")
dbutils.widgets.text("source_system_id", "")
dbutils.widgets.text("schema_name", "")
dbutils.widgets.text("table_name", "")
dbutils.widgets.text("primary_key_columns", "")
dbutils.widgets.text("run_id", "")
dbutils.widgets.text("rows_copied", "0")

table_id = dbutils.widgets.get("table_id")
source_system = dbutils.widgets.get("source_system_id")
schema = dbutils.widgets.get("schema_name")
table = dbutils.widgets.get("table_name")
pks = dbutils.widgets.get("primary_key_columns")
run_id = dbutils.widgets.get("run_id")
rows_copied = int(dbutils.widgets.get("rows_copied"))

# PLACEHOLDERS
SQL_SERVER = "adf-databricks.database.windows.net"
SQL_DB = "ADF-Databricks"
SQL_USER = "YOUR_SQL_USERNAME"
SQL_PASS = "YOUR_SQL_PASSWORD"

# 2. Paths
bronze_path = f"/mnt/datalake/bronze/{source_system}/{schema}/{table}/incremental/{run_id}"
silver_path = f"/mnt/datalake/silver/{source_system}/{schema}/{table}"

try:
    # 3. Read CDC Data
    df_cdc = spark.read.parquet(bronze_path)

    # Get Max Version for Control Update
    max_version = df_cdc.agg({"_current_sync_version": "max"}).collect()[0][0]

    # 4. Prepare for Merge
    df_cdc_ready = df_cdc \
        .withColumn("_load_timestamp", current_timestamp()) \
        .withColumn("_source_system_id", lit(source_system)) \
        .withColumn("_run_id", lit(run_id))

    # 5. Build Dynamic Merge Condition
    # Handle composite keys (e.g. "CITY,STATE" -> "target.CITY=source.CITY AND target.STATE=source.STATE")
    pk_list = [pk.strip() for pk in pks.split(',')]
    merge_condition = " AND ".join([f"target.{pk} = source.{pk}" for pk in pk_list])

    delta_table = DeltaTable.forPath(spark, silver_path)

    # 6. Execute Merge
    delta_table.alias("target").merge(
        df_cdc_ready.alias("source"),
        merge_condition
    ).whenMatchedUpdate(
        condition = "source.SYS_CHANGE_OPERATION IN ('U', 'I')",
        set = {c: f"source.{c}" for c in df_cdc_ready.columns if c not in ['SYS_CHANGE_OPERATION', 'SYS_CHANGE_VERSION']}
    ).whenMatchedUpdate(
        condition = "source.SYS_CHANGE_OPERATION = 'D'",
        set = {"_is_deleted": "lit(True)", "_run_id": f"'{run_id}'"}
    ).whenNotMatchedInsert(
        condition = "source.SYS_CHANGE_OPERATION IN ('I', 'U')",
        values = {c: f"source.{c}" for c in df_cdc_ready.columns if c not in ['SYS_CHANGE_OPERATION', 'SYS_CHANGE_VERSION']}
    ).execute()

    # 7. Update Control Table
    conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SQL_SERVER};DATABASE={SQL_DB};UID={SQL_USER};PWD={SQL_PASS}"
    with pyodbc.connect(conn_str) as conn:
        with conn.cursor() as cursor:
            sql = f"""
            EXEC control.sp_UpdateTableMetadata
                @TableId = '{table_id}',
                @LastLoadStatus = 'success',
                @RowsProcessed = {rows_copied},
                @LastSyncVersion = {max_version},
                @RunId = '{run_id}'
            """
            cursor.execute(sql)
            conn.commit()

    dbutils.notebook.exit(json.dumps({"status": "success", "rows": rows_copied}))

except Exception as e:
    conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SQL_SERVER};DATABASE={SQL_DB};UID={SQL_USER};PWD={SQL_PASS}"
    with pyodbc.connect(conn_str) as conn:
        with conn.cursor() as cursor:
            sql = f"EXEC control.sp_UpdateTableMetadata @TableId = '{table_id}', @LastLoadStatus = 'failed', @RunId = '{run_id}'"
            cursor.execute(sql)
            conn.commit()
    raise e
