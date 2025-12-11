/*
  POPULATION SCRIPT
  Based on Metadata Catalog: Customer Lifetime Value (CLV) Analytics
*/

-- 1. Source Systems
INSERT INTO control.source_systems (source_system_id, source_system_name, description) VALUES
('SRC-001', 'Azure SQL ERP', 'Orders, Inventory, Finance'),
('SRC-002', 'Azure SQL CRM', 'Customer Master, Support'),
('SRC-003', 'Azure SQL Marketing', 'Campaign Management');

-- 2. Table Metadata
-- Note: initial_load_completed defaults to 0
INSERT INTO control.table_metadata (table_id, source_system_id, schema_name, table_name, primary_key_columns) VALUES
-- CRM Tables
('ENT-001', 'SRC-002', 'CRM', 'Customers', 'CUSTOMER_ID'),
('ENT-002', 'SRC-002', 'CRM', 'CustomerRegistrationSource', 'REGISTRATION_SOURCE_ID'),
('ENT-011', 'SRC-002', 'CRM', 'INCIDENTS', 'INCIDENT_ID'),
('ENT-012', 'SRC-002', 'CRM', 'INTERACTIONS', 'INTERACTION_ID'),
('ENT-013', 'SRC-002', 'CRM', 'SURVEYS', 'SURVEY_ID'),

-- ERP Tables
('ENT-003', 'SRC-001', 'ERP', 'OE_ORDER_HEADERS_ALL', 'ORDER_ID'),
('ENT-004', 'SRC-001', 'ERP', 'OE_ORDER_LINES_ALL', 'LINE_ID'),
('ENT-005', 'SRC-001', 'ERP', 'ADDRESSES', 'ADDRESS_ID'),
('ENT-006', 'SRC-001', 'ERP', 'CITY_TIER_MASTER', 'CITY,STATE'), -- Composite PK
('ENT-007', 'SRC-001', 'ERP', 'MTL_SYSTEM_ITEMS_B', 'INVENTORY_ITEM_ID'),
('ENT-008', 'SRC-001', 'ERP', 'CATEGORIES', 'CATEGORY_ID'),
('ENT-009', 'SRC-001', 'ERP', 'BRANDS', 'BRAND_ID'),

-- Marketing Tables
('ENT-010', 'SRC-003', 'MARKETING', 'MARKETING_CAMPAIGNS', 'CAMPAIGN_ID');

-- 3. Load Dependencies
-- Logic: Parents load before Children
INSERT INTO control.load_dependencies (table_id, depends_on_table_id) VALUES
-- Registration needs Customer & Campaigns
('ENT-002', 'ENT-001'), ('ENT-002', 'ENT-010'),

-- Addresses needs Customers (Customer owns address)
('ENT-005', 'ENT-001'),
-- Addresses needs City Tier (for validation/lookup if enforced)
('ENT-005', 'ENT-006'),

-- Items needs Category & Brand
('ENT-007', 'ENT-008'), ('ENT-007', 'ENT-009'),

-- Orders needs Customers & Addresses
('ENT-003', 'ENT-001'), ('ENT-003', 'ENT-005'),

-- Order Lines needs Orders & Items
('ENT-004', 'ENT-003'), ('ENT-004', 'ENT-007'),

-- Incidents needs Customers & Orders
('ENT-011', 'ENT-001'), ('ENT-011', 'ENT-003'),

-- Interactions needs Incidents
('ENT-012', 'ENT-011'),

-- Surveys needs Customers, Orders, Incidents
('ENT-013', 'ENT-001'), ('ENT-013', 'ENT-003'), ('ENT-013', 'ENT-011');
GO
