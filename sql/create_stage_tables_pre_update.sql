-- ===============================================
-- STAGE LAYER TABLES
-- Raw data from API / CSV files
-- Each table stores raw datasets for further ETL processing
-- ===============================================

-- ----------------------------
-- Customer Research Table
-- Stores sales data by category, date, and geography
-- ----------------------------
DROP TABLE IF EXISTS STAGE.CUSTOMER_RESEARCH;

CREATE TABLE STAGE.CUSTOMER_RESEARCH (
    ID SERIAL PRIMARY KEY,
    CATEGORY_ID INT,
    DATE_ID TIMESTAMP,
    GEO_ID INT,
    SALES_QTY INT,
    SALES_AMT NUMERIC(14,2)
);

-- ----------------------------
-- User Activity Log Table
-- Stores user actions and interactions on the platform
-- ----------------------------
DROP TABLE IF EXISTS STAGE.USER_ACTIVITY_LOG;

CREATE TABLE STAGE.USER_ACTIVITY_LOG (
    ID SERIAL PRIMARY KEY,
    DATE_TIME TIMESTAMP NOT NULL,
    ACTION_ID BIGINT NOT NULL,
    CUSTOMER_ID BIGINT NOT NULL,
    QUANTITY BIGINT
);

-- ----------------------------
-- User Order Log Table
-- Stores individual user purchase records
-- ----------------------------
DROP TABLE IF EXISTS STAGE.USER_ORDER_LOG;

CREATE TABLE STAGE.USER_ORDER_LOG (
    ID SERIAL PRIMARY KEY,
    DATE_TIME TIMESTAMP NOT NULL,
    CITY_ID INT,
    CITY_NAME VARCHAR(100),
    CUSTOMER_ID BIGINT,
    FIRST_NAME VARCHAR(100),
    LAST_NAME VARCHAR(100),
    ITEM_ID INT,
    ITEM_NAME VARCHAR(100),
    QUANTITY BIGINT,
    PAYMENT_AMOUNT NUMERIC(14,2)
);