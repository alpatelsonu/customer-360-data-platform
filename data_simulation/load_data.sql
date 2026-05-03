USE ROLE ACCOUNTADMIN;

-- ==========================================
-- 1. CREATE DATABASE & SCHEMA
-- ==========================================

CREATE OR REPLACE DATABASE CUSTOMER_360;
CREATE OR REPLACE SCHEMA CUSTOMER_360.RAW;

--  ==============================================
--  2. CREATE FILE FORMATS
--  ==============================================

CREATE OR REPLACE FILE FORMAT CUSTOMER_360.RAW.csv_format
TYPE = CSV
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
SKIP_HEADER = 1;

CREATE OR REPLACE FILE FORMAT CUSTOMER_360.RAW.json_format
TYPE = JSON;

--  ==============================================
--  3. CREATE STAGE
--  ==============================================

CREATE OR REPLACE STAGE CUSTOMER_360.RAW.raw_stage;

--  ==============================================
--  4. CREATE TABLES
--  ==============================================

CREATE OR REPLACE TABLE CUSTOMER_360.RAW.raw_orders (
    order_id STRING,
    user_id STRING,
    order_amount NUMBER,
    order_status STRING,
    order_timestamp TIMESTAMP,
    order_priority STRING,
    ingestion_timestamp TIMESTAMP,
    source_file STRING
);

CREATE OR REPLACE TABLE CUSTOMER_360.RAW.raw_user_events (
    event_id STRING,
    event_type STRING,
    user_id STRING,
    device_id STRING,
    session_id STRING,
    product_id STRING,
    event_timestamp TIMESTAMP,
    ingestion_timestamp TIMESTAMP,
    source_file STRING
);

CREATE OR REPLACE TABLE CUSTOMER_360.RAW.raw_identity_map (
    old_user_id STRING,
    new_user_id STRING,
    effective_from TIMESTAMP,
    ingestion_timestamp TIMESTAMP,
    source_file STRING
);

--  ==============================================
--  4. UPLOAD FILES (RUN VIA SNOWSQL)
--  ==============================================

-- PUT file://orders.csv @CUSTOMER_360.RAW.raw_stage;
-- PUT file://user_events.json @CUSTOMER_360.RAW.raw_stage;
-- PUT file://swap_events.csv @CUSTOMER_360.RAW.raw_stage;

--  ==============================================
--  4. LOAD ORDERS DATA
--  ==============================================

COPY INTO CUSTOMER_360.RAW.raw_orders
FROM (
    SELECT
        t.$1 AS order_id,
        t.$2 AS user_id,
        t.$3 AS order_amount,
        t.$4 AS order_status,
        t.$5 AS order_timestamp,
        t.$6 AS order_priority,
        CURRENT_TIMESTAMP() AS ingestion_timestamp,
        METADATA$FILENAME AS source_file
    FROM @CUSTOMER_360.RAW.raw_stage t
)
FILE_FORMAT = CUSTOMER_360.RAW.csv_format
PATTERN = '.*orders.*';

--  ==============================================
--  4. LOAD USER EVENTS DATA
--  ==============================================

COPY INTO CUSTOMER_360.RAW.raw_user_events
FROM (
    SELECT 
        t.$1:event_id::STRING,
        t.$1:event_type::STRING,
        t.$1:user_id::STRING,
        t.$1:device_id::STRING,
        t.$1:session_id::STRING,
        t.$1:product_id::STRING,
        t.$1:event_timestamp::TIMESTAMP,
        CURRENT_TIMESTAMP(),
        METADATA$FILENAME
    FROM @CUSTOMER_360.RAW.raw_stage t
)
FILE_FORMAT = CUSTOMER_360.RAW.json_format
PATTERN = '.*user_events.*';

--  ==============================================
--  4. LOAD SWAP EVENTS DATA
--  ==============================================

COPY INTO CUSTOMER_360.RAW.raw_identity_map
FROM (
    SELECT 
        t.$1 AS old_user_id,
        t.$2 AS new_user_id,
        t.$3 AS effective_from,
        CURRENT_TIMESTAMP(),
        METADATA$FILENAME
    FROM @CUSTOMER_360.RAW.raw_stage t
)
FILE_FORMAT = CUSTOMER_360.RAW.csv_format
PATTERN = '.*swap_event.*';

-- =========================================
-- 9. VALIDATION
-- =========================================
SELECT COUNT(*) FROM CUSTOMER_360.RAW.raw_orders;
SELECT COUNT(*) FROM CUSTOMER_360.RAW.raw_user_events;
SELECT COUNT(*) FROM CUSTOMER_360.RAW.raw_identity_map;

-- ============================================
-- 9. FIX USER_ID IN ORDERS TABLE AFTER GID SWAP
-- =============================================

-- ============================================
-- 10. CREATE USER WITH ROLE AND GRANT PERMISSION BASED ON RBAC
-- =============================================

CREATE USER IF NOT EXISTS dbt_sopatel SET MUST_CHANGE_PASSWORD = TRUE;
ALTER USER sopatel_dbt SET PASSWORD='***'; -- only for dev

CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH 
AUTO_SUPEND = 300;

CREATE ROLE IF NOT EXISTS dbt_role;
GRANT ALL ON DATABASE CUSTOMER_360 TO dbt_role;
GRANT USAGE ON SCHEMA CUSTOMER_360.RAW TO DBT_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA CUSTOMER_360.RAW TO ROLE DBT_ROLE; 
GRANT SELECT ON FUTURE TABLES IN SCHEMA CUSTOMER_360.RAW TO ROLE DBT_ROLE; 

GRANT USAGE ON WAREHOUSE COMPUTE_WH TO dbt_role;

GRANT ROLE dbt_role TO USER dbt_sopatel;