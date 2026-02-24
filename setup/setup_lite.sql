-- ==============================================================================
-- SNOWOPS INTEL LITE — SETUP SCRIPT
-- ==============================================================================
-- Run as ACCOUNTADMIN in a Snowsight worksheet.
-- Creates database, schemas, tables, warehouse, and optional telemetry config.
--
-- Telemetry: Anonymous usage data helps us improve the product.
--            Set TELEMETRY_ENABLED to FALSE to disable.
-- ==============================================================================

USE ROLE ACCOUNTADMIN;

-- ╔══════════════════════════════════════════════════════════════════════╗
-- ║ 1. INFRASTRUCTURE                                                  ║
-- ╚══════════════════════════════════════════════════════════════════════╝

CREATE DATABASE IF NOT EXISTS SNOWFLAKE_OPS_INTELLIGENCE;
USE DATABASE SNOWFLAKE_OPS_INTELLIGENCE;

CREATE SCHEMA IF NOT EXISTS APP_DATA;
CREATE SCHEMA IF NOT EXISTS APP_CONTEXT;
CREATE SCHEMA IF NOT EXISTS APP_ANALYTICS;

-- Warehouse (X-Small, cost-efficient)
CREATE WAREHOUSE IF NOT EXISTS SNOWOPS_WH
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;

USE WAREHOUSE SNOWOPS_WH;


-- ╔══════════════════════════════════════════════════════════════════════╗
-- ║ 2. CORE TABLES                                                     ║
-- ╚══════════════════════════════════════════════════════════════════════╝

CREATE TABLE IF NOT EXISTS APP_CONTEXT.PLATFORM_SETTINGS (
    SETTING_KEY VARCHAR(100) PRIMARY KEY,
    SETTING_VALUE VARCHAR(1000),
    DESCRIPTION VARCHAR(500),
    UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS APP_CONTEXT.WAREHOUSE_CONTEXT (
    WAREHOUSE_ID NUMBER AUTOINCREMENT PRIMARY KEY,
    WAREHOUSE_NAME VARCHAR(255) NOT NULL UNIQUE,
    PURPOSE VARCHAR(50) DEFAULT 'GENERAL',
    SIZE VARCHAR(20),
    COST_PROFILE VARCHAR(20) DEFAULT 'BALANCED',
    OWNER_TEAM VARCHAR(255),
    NOTES VARCHAR(2000),
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS APP_CONTEXT.BUDGET_ALERTS (
    ALERT_ID NUMBER AUTOINCREMENT PRIMARY KEY,
    ALERT_NAME VARCHAR(255) NOT NULL,
    ALERT_TYPE VARCHAR(50),
    TARGET_NAME VARCHAR(255),
    THRESHOLD_CREDITS FLOAT,
    THRESHOLD_PERCENTAGE FLOAT,
    THRESHOLD_VALUE FLOAT,
    CONDITION_OP VARCHAR(50) DEFAULT '>',
    IS_ACTIVE BOOLEAN DEFAULT TRUE,
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS APP_ANALYTICS.DAILY_COST_SNAPSHOT (
    SNAPSHOT_DATE DATE PRIMARY KEY,
    TOTAL_CREDITS_USED FLOAT,
    COMPUTE_CREDITS FLOAT,
    STORAGE_CREDITS FLOAT,
    CLOUD_SERVICES_CREDITS FLOAT,
    DATA_TRANSFER_CREDITS FLOAT,
    WAREHOUSE_COUNT NUMBER,
    QUERY_COUNT NUMBER,
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS APP_ANALYTICS.WAREHOUSE_DAILY_METRICS (
    METRIC_ID NUMBER AUTOINCREMENT PRIMARY KEY,
    METRIC_DATE DATE,
    WAREHOUSE_NAME VARCHAR(255),
    CREDITS_USED FLOAT,
    QUERY_COUNT NUMBER,
    AVG_EXECUTION_TIME_MS FLOAT,
    CACHE_HIT_RATIO FLOAT,
    BYTES_SCANNED NUMBER,
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UNIQUE (METRIC_DATE, WAREHOUSE_NAME)
);

CREATE TABLE IF NOT EXISTS APP_ANALYTICS.QUERY_BENCHMARK (
    BENCHMARK_ID NUMBER AUTOINCREMENT PRIMARY KEY,
    QUERY_TEXT VARCHAR(10000),
    QUERY_HASH VARCHAR(64),
    RUN_TYPE VARCHAR(20),
    PREDICTED_COST_CREDITS FLOAT,
    ACTUAL_COST_CREDITS FLOAT,
    RUN_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);


-- ╔══════════════════════════════════════════════════════════════════════╗
-- ║ 3. DEFAULT SETTINGS                                                ║
-- ╚══════════════════════════════════════════════════════════════════════╝

INSERT INTO APP_CONTEXT.PLATFORM_SETTINGS (SETTING_KEY, SETTING_VALUE, DESCRIPTION)
SELECT 'COST_PER_CREDIT', '3.00', 'Dollar cost per Snowflake credit'
WHERE NOT EXISTS (SELECT 1 FROM APP_CONTEXT.PLATFORM_SETTINGS WHERE SETTING_KEY = 'COST_PER_CREDIT');

INSERT INTO APP_CONTEXT.PLATFORM_SETTINGS (SETTING_KEY, SETTING_VALUE, DESCRIPTION)
SELECT 'MONTHLY_BUDGET_CREDITS', '1000', 'Monthly credit budget limit'
WHERE NOT EXISTS (SELECT 1 FROM APP_CONTEXT.PLATFORM_SETTINGS WHERE SETTING_KEY = 'MONTHLY_BUDGET_CREDITS');


-- ╔══════════════════════════════════════════════════════════════════════╗
-- ║ 4. TELEMETRY (Anonymous usage tracking — disable with FALSE)       ║
-- ╚══════════════════════════════════════════════════════════════════════╝
-- Anonymous telemetry helps us understand which features are used most
-- and improve the product. No PII, query text, or cost data is collected.
-- Disable by setting TELEMETRY_ENABLED to FALSE below.

INSERT INTO APP_CONTEXT.PLATFORM_SETTINGS (SETTING_KEY, SETTING_VALUE, DESCRIPTION)
SELECT 'TELEMETRY_ENABLED', 'TRUE', 'Enable anonymous usage telemetry (TRUE/FALSE). Helps improve the product.'
WHERE NOT EXISTS (SELECT 1 FROM APP_CONTEXT.PLATFORM_SETTINGS WHERE SETTING_KEY = 'TELEMETRY_ENABLED');

INSERT INTO APP_CONTEXT.PLATFORM_SETTINGS (SETTING_KEY, SETTING_VALUE, DESCRIPTION)
SELECT 'POSTHOG_API_KEY', 'phc_W89NRd31nyEXwMDNHgvW1kxycaKPLq2SqKjm9RuKpzH', 'PostHog project API key for anonymous telemetry'
WHERE NOT EXISTS (SELECT 1 FROM APP_CONTEXT.PLATFORM_SETTINGS WHERE SETTING_KEY = 'POSTHOG_API_KEY');

INSERT INTO APP_CONTEXT.PLATFORM_SETTINGS (SETTING_KEY, SETTING_VALUE, DESCRIPTION)
SELECT 'POSTHOG_HOST', 'https://us.i.posthog.com', 'PostHog API endpoint'
WHERE NOT EXISTS (SELECT 1 FROM APP_CONTEXT.PLATFORM_SETTINGS WHERE SETTING_KEY = 'POSTHOG_HOST');


-- ╔══════════════════════════════════════════════════════════════════════╗
-- ║ 5. GRANTS                                                          ║
-- ╚══════════════════════════════════════════════════════════════════════╝

GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE PUBLIC;
GRANT USAGE ON DATABASE SNOWFLAKE_OPS_INTELLIGENCE TO ROLE PUBLIC;
GRANT USAGE ON ALL SCHEMAS IN DATABASE SNOWFLAKE_OPS_INTELLIGENCE TO ROLE PUBLIC;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA APP_CONTEXT TO ROLE PUBLIC;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA APP_ANALYTICS TO ROLE PUBLIC;
GRANT SELECT, INSERT, UPDATE, DELETE ON FUTURE TABLES IN SCHEMA APP_CONTEXT TO ROLE PUBLIC;
GRANT SELECT, INSERT, UPDATE, DELETE ON FUTURE TABLES IN SCHEMA APP_ANALYTICS TO ROLE PUBLIC;


-- ╔══════════════════════════════════════════════════════════════════════╗
-- ║ 6. EXTERNAL ACCESS (Optional — for telemetry)                      ║
-- ╚══════════════════════════════════════════════════════════════════════╝
-- Uncomment below if you want telemetry to work in Streamlit-in-Snowflake.
-- This creates a network rule allowing outbound HTTPS to PostHog.

-- CREATE OR REPLACE NETWORK RULE APP_CONTEXT.POSTHOG_NETWORK_RULE
--   TYPE = HOST_PORT  MODE = EGRESS
--   VALUE_LIST = ('us.i.posthog.com:443', 'us-assets.i.posthog.com:443');
--
-- CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION POSTHOG_ACCESS
--   ALLOWED_NETWORK_RULES = (SNOWFLAKE_OPS_INTELLIGENCE.APP_CONTEXT.POSTHOG_NETWORK_RULE)
--   ENABLED = TRUE;


-- ╔══════════════════════════════════════════════════════════════════════╗
-- ║ SETUP COMPLETE                                                     ║
-- ╚══════════════════════════════════════════════════════════════════════╝

SELECT '✅ SnowOps Intel Lite setup complete!' AS STATUS;
