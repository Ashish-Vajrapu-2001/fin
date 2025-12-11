# Metadata-Driven CDC Pipeline (ADF + Databricks)

## Architecture
This framework uses Azure Data Factory to orchestrate data movement based on metadata stored in Azure SQL. It handles both initial full loads and incremental Change Data Capture (CDC) processing using SQL Change Tracking.

### Flow
1. **Master Pipeline**: Queries `control.sp_GetTableLoadOrder` to get list of tables.
2. **Logic Check**: Checks `initial_load_completed` flag.
3. **Dispatch**:
   - If `False` -> Runs Initial Load Pipeline (Copy Full Table -> Write Delta).
   - If `True` -> Runs Incremental CDC Pipeline (Get Changes -> Merge Delta).
4. **Processing**: Databricks handles the heavy lifting of Schema Evolution and Merges.
5. **Feedback**: Databricks updates `control.table_metadata` with success/failure and new Sync Versions.

## Adding New Tables
No code changes required! Just SQL:
```sql
INSERT INTO control.table_metadata (table_id, source_system_id, schema_name, table_name, primary_key_columns)
VALUES ('NEW-001', 'SRC-001', 'Sales', 'NewTable', 'ID');
```

## Critical Guardrails
1. **Boolean Logic**: The ADF `IfCondition` checks `@equals(..., false)`. Do not invert this.
2. **CDC Selection**: The stored proc `sp_GetCDCChanges` automatically handles `SYS_CHANGE` columns. Do not manually select them to avoid duplicates.
3. **BIT Casting**: The control API casts flags to BIT to ensure ADF sees strict Booleans.
