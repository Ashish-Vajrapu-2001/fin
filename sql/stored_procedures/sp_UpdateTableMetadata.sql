CREATE OR ALTER PROCEDURE control.sp_UpdateTableMetadata
    @TableId NVARCHAR(50),
    @LastLoadStatus NVARCHAR(20),
    @LastLoadTimestamp DATETIME2 = NULL,
    @RowsProcessed INT = NULL,
    @InitialLoadCompleted BIT = NULL,
    @LastSyncVersion BIGINT = NULL,
    @RunId NVARCHAR(100) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET @LastLoadTimestamp = ISNULL(@LastLoadTimestamp, GETDATE());

    BEGIN TRANSACTION;

    -- 1. Update Metadata
    UPDATE control.table_metadata
    SET
        last_load_status = @LastLoadStatus,
        last_load_timestamp = @LastLoadTimestamp,
        rows_processed_last_run = ISNULL(@RowsProcessed, rows_processed_last_run),
        initial_load_completed = ISNULL(@InitialLoadCompleted, initial_load_completed),
        last_sync_version = ISNULL(@LastSyncVersion, last_sync_version),
        last_run_id = ISNULL(@RunId, last_run_id),
        updated_at = GETDATE()
    WHERE table_id = @TableId;

    -- 2. Log Execution
    INSERT INTO control.pipeline_execution_log (
        run_id, table_id, load_type, start_time, end_time, status, rows_read
    )
    VALUES (
        ISNULL(@RunId, 'UNKNOWN'),
        @TableId,
        CASE WHEN @InitialLoadCompleted = 1 THEN 'INITIAL' ELSE 'CDC' END,
        @LastLoadTimestamp, -- Approximate start
        GETDATE(),
        @LastLoadStatus,
        @RowsProcessed
    );

    COMMIT TRANSACTION;
END
