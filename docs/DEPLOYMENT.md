# Deployment Checklist

## Prerequisites
1. Azure SQL Database created.
2. ADLS Gen2 Storage Account created.
3. Azure Databricks Workspace created.
4. Key Vault (recommended) or Placeholders updated.

## Step 1: SQL Setup
1. Connect to Azure SQL (`adf-databricks.database.windows.net`).
2. Run `01_create_control_tables.sql`.
3. Run `02_populate_control_tables.sql`.
4. Run all 3 Stored Procedures.

## Step 2: Source Database Setup
Enable Change Tracking on your **SOURCE** database (ERP/CRM):
```sql
ALTER DATABASE [SourceDB] SET CHANGE_TRACKING = ON (CHANGE_RETENTION = 2 DAYS, AUTO_CLEANUP = ON);
ALTER TABLE CRM.Customers ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = ON);
-- Repeat for all tables in metadata
```

## Step 3: Databricks Setup
1. Create a Cluster (Runtime 10.4 LTS or higher).
2. Install Library: `pyodbc` (via PyPI).
3. Import Notebooks from `databricks/notebooks/`.
4. Run `setup/Mount_ADLS` once.

## Step 4: ADF Setup
1. Create Linked Services (update credentials).
2. Create Datasets.
3. Import Pipelines.
4. Debug `PL_Master_Orchestrator`.
