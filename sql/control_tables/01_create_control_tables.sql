/*
  SCHEMA: CONTROL LAYER
  Purpose: Stores metadata, dependencies, and execution logs.
*/

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'control')
BEGIN
    EXEC('CREATE SCHEMA [control]')
END
GO

-- 1. Source Systems Registry
CREATE TABLE control.source_systems (
    source_system_id NVARCHAR(50) NOT NULL, -- e.g., 'SRC-001'
    source_system_name NVARCHAR(100) NOT NULL,
    description NVARCHAR(500),
    is_active BIT DEFAULT 1,
    created_at DATETIME2 DEFAULT GETDATE(),
    updated_at DATETIME2 DEFAULT GETDATE(),
    CONSTRAINT PK_source_systems PRIMARY KEY (source_system_id)
);

-- 2. Table Metadata (CORE TABLE)
CREATE TABLE control.table_metadata (
    table_id NVARCHAR(50) NOT NULL, -- e.g., 'ENT-001'
    source_system_id NVARCHAR(50) NOT NULL,
    schema_name NVARCHAR(128) NOT NULL,
    table_name NVARCHAR(128) NOT NULL,
    primary_key_columns NVARCHAR(500) NOT NULL, -- Comma separated: 'ID' or 'OrderID,LineID'

    -- Status Flags
    is_active BIT DEFAULT 1,
    initial_load_completed BIT DEFAULT 0, -- 0=Needs Initial, 1=Needs CDC

    -- Load Status
    last_load_status NVARCHAR(20), -- 'success', 'failed', 'running'
    last_load_timestamp DATETIME2,
    rows_processed_last_run INT,

    -- Traceability
    last_run_id NVARCHAR(100),
    last_sync_version BIGINT, -- SQL Change Tracking Version

    created_at DATETIME2 DEFAULT GETDATE(),
    updated_at DATETIME2 DEFAULT GETDATE(),

    CONSTRAINT PK_table_metadata PRIMARY KEY (table_id),
    CONSTRAINT FK_table_metadata_source FOREIGN KEY (source_system_id)
        REFERENCES control.source_systems(source_system_id),
    CONSTRAINT UQ_schema_table UNIQUE (source_system_id, schema_name, table_name)
);

-- Indexes for performance
CREATE INDEX IX_table_metadata_active ON control.table_metadata(is_active) INCLUDE (initial_load_completed);
CREATE INDEX IX_table_metadata_source ON control.table_metadata(source_system_id);

-- 3. Load Dependencies (Topological Sort Support)
CREATE TABLE control.load_dependencies (
    dependency_id INT IDENTITY(1,1) PRIMARY KEY,
    table_id NVARCHAR(50) NOT NULL, -- Child
    depends_on_table_id NVARCHAR(50) NOT NULL, -- Parent
    created_at DATETIME2 DEFAULT GETDATE(),

    CONSTRAINT FK_dep_child FOREIGN KEY (table_id) REFERENCES control.table_metadata(table_id),
    CONSTRAINT FK_dep_parent FOREIGN KEY (depends_on_table_id) REFERENCES control.table_metadata(table_id),
    CONSTRAINT UQ_dependency UNIQUE (table_id, depends_on_table_id)
);

-- 4. Pipeline Execution Log (Audit Trail)
CREATE TABLE control.pipeline_execution_log (
    log_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    run_id NVARCHAR(100) NOT NULL,
    table_id NVARCHAR(50) NOT NULL,
    load_type NVARCHAR(20) NOT NULL, -- 'INITIAL' or 'INCREMENTAL'
    start_time DATETIME2 DEFAULT GETDATE(),
    end_time DATETIME2,
    status NVARCHAR(20),
    rows_read INT,
    rows_written INT,
    error_message NVARCHAR(MAX),

    CONSTRAINT FK_log_table FOREIGN KEY (table_id) REFERENCES control.table_metadata(table_id)
);

CREATE INDEX IX_pipeline_log_table_date ON control.pipeline_execution_log(table_id, start_time DESC);
