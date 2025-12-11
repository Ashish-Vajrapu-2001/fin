CREATE OR ALTER PROCEDURE control.sp_GetTableLoadOrder
    @SourceSystemId NVARCHAR(50) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    -- CHECKPOINT #3: Explicit casting of BIT types for ADF JSON compatibility

    ;WITH DependencyGraph AS (
        -- Base Case: Tables with NO dependencies (Level 0)
        SELECT
            tm.table_id,
            tm.source_system_id,
            ss.source_system_name,
            tm.schema_name,
            tm.table_name,
            tm.primary_key_columns,
            CAST(tm.initial_load_completed AS BIT) AS initial_load_completed,
            0 AS dependency_level
        FROM control.table_metadata tm
        JOIN control.source_systems ss ON tm.source_system_id = ss.source_system_id
        LEFT JOIN control.load_dependencies ld ON tm.table_id = ld.table_id
        WHERE tm.is_active = 1
          AND ss.is_active = 1
          AND ld.depends_on_table_id IS NULL

        UNION ALL

        -- Recursive Case: Tables depending on Level N
        SELECT
            tm.table_id,
            tm.source_system_id,
            ss.source_system_name,
            tm.schema_name,
            tm.table_name,
            tm.primary_key_columns,
            CAST(tm.initial_load_completed AS BIT) AS initial_load_completed,
            parent.dependency_level + 1
        FROM control.table_metadata tm
        JOIN control.source_systems ss ON tm.source_system_id = ss.source_system_id
        JOIN control.load_dependencies ld ON tm.table_id = ld.table_id
        JOIN DependencyGraph parent ON ld.depends_on_table_id = parent.table_id
        WHERE tm.is_active = 1
    )
    SELECT DISTINCT
        table_id,
        source_system_id,
        source_system_name,
        schema_name,
        table_name,
        primary_key_columns,
        initial_load_completed,
        dependency_level
    FROM DependencyGraph
    WHERE (@SourceSystemId IS NULL OR source_system_id = @SourceSystemId)
    ORDER BY dependency_level ASC, table_name ASC;
END
