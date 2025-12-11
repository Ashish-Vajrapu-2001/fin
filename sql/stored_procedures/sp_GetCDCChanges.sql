CREATE OR ALTER PROCEDURE control.sp_GetCDCChanges
    @SchemaName NVARCHAR(128),
    @TableName NVARCHAR(128),
    @TableId NVARCHAR(50)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @LastSyncVersion BIGINT;
    DECLARE @CurrentVersion BIGINT;
    DECLARE @PKColumns NVARCHAR(500);
    DECLARE @SQL NVARCHAR(MAX);
    DECLARE @JoinCondition NVARCHAR(MAX) = '';

    -- 1. Get Metadata
    SELECT
        @LastSyncVersion = ISNULL(last_sync_version, 0),
        @PKColumns = primary_key_columns
    FROM control.table_metadata
    WHERE table_id = @TableId;

    -- 2. Get Current Version
    SET @CurrentVersion = CHANGE_TRACKING_CURRENT_VERSION();

    -- 3. Build Join Condition (Handles Composite Keys)
    SELECT @JoinCondition = STRING_AGG(
        CAST('CT.' + TRIM(value) + ' = T.' + TRIM(value) AS NVARCHAR(MAX)),
        ' AND '
    )
    FROM STRING_SPLIT(@PKColumns, ',');

    -- 4. Construct Dynamic SQL
    -- CHECKPOINT #2: Do NOT select SYS_ columns explicitly, CT.* has them.
    -- Added _current_sync_version as a literal for the target system tracking.
    SET @SQL = '
    SELECT
        CT.*,
        ' + CAST(@CurrentVersion AS NVARCHAR(20)) + ' AS _current_sync_version,
        T.*
    FROM CHANGETABLE(CHANGES ' + @SchemaName + '.' + @TableName + ', ' + CAST(@LastSyncVersion AS NVARCHAR(20)) + ') AS CT
    LEFT JOIN ' + @SchemaName + '.' + @TableName + ' AS T
        ON ' + @JoinCondition;

    PRINT 'Executing CDC Query for version > ' + CAST(@LastSyncVersion AS NVARCHAR(20));

    BEGIN TRY
        EXEC sp_executesql @SQL;
    END TRY
    BEGIN CATCH
        THROW;
    END CATCH
END
