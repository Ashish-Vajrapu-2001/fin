# Initial_Load_Dynamic.py
import pyodbc
import json
from pyspark.sql.functions import current_timestamp, lit
from delta.tables import *

# 1. Inputs
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

# PLACEHOLDERS - Replace with Key Vault refs in prod
SQL_SERVER = "adf-databricks.database.windows.net"
SQL_DB = "ADF-Databricks"
SQL_USER = "YOUR_SQL_USERNAME"
SQL_PASS = "YOUR_SQL_PASSWORD"

# 2. Paths
bronze_path = f"/mnt/datalake/bronze/{source_system}/{schema}/{table}/initial_load/{run_id}"
silver_path = f"/mnt/datalake/silver/{source_system}/{schema}/{table}"

try:
    if rows_copied > 0:
        # 3. Read & Transform
        df = spark.read.parquet(bronze_path)

        # Metadata Enrichment
        df_final = df \
            .withColumn("_load_timestamp", current_timestamp()) \
            .withColumn("_source_system_id", lit(source_system)) \
            .withColumn("_run_id", lit(run_id)) \
            .withColumn("_is_deleted", lit(False)) \
            .withColumn("_current_sync_version", lit(0)) # Initial load

        # 4. Write Delta (Overwrite for initial)
        df_final.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(silver_path)

        # Optimize
        spark.sql(f"OPTIMIZE delta.`{silver_path}`")

    # 5. Update Control Table (Using PyODBC for DML)
    conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SQL_SERVER};DATABASE={SQL_DB};UID={SQL_USER};PWD={SQL_PASS}"
    with pyodbc.connect(conn_str) as conn:
        with conn.cursor() as cursor:
            sql = f"""
            EXEC control.sp_UpdateTableMetadata
                @TableId = '{table_id}',
                @LastLoadStatus = 'success',
                @RowsProcessed = {rows_copied},
                @InitialLoadCompleted = 1,
                @LastSyncVersion = 0,
                @RunId = '{run_id}'
            """
            cursor.execute(sql)
            conn.commit()

    dbutils.notebook.exit(json.dumps({"status": "success", "table": f"{schema}.{table}"}))

except Exception as e:
    # Log failure to control table
    with pyodbc.connect(conn_str) as conn:
        with conn.cursor() as cursor:
            sql = f"EXEC control.sp_UpdateTableMetadata @TableId = '{table_id}', @LastLoadStatus = 'failed', @RunId = '{run_id}'"
            cursor.execute(sql)
            conn.commit()
    raise e
