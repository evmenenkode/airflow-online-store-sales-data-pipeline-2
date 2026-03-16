-- ===============================================
-- MART LAYER TABLES
-- Dimension (D_*) and Fact (F_*) tables
-- Structured for analytics and reporting
-- ===============================================

-- ----------------------------
-- Drop existing tables if they exist
-- ----------------------------
DROP TABLE IF EXISTS MART.F_RESEARCH;
DROP TABLE IF EXISTS MART.F_DAILY_SALES;
DROP TABLE IF EXISTS MART.F_ACTIVITY;
DROP TABLE IF EXISTS MART.D_CALENDAR;
DROP TABLE IF EXISTS MART.D_CATEGORY;
DROP TABLE IF EXISTS MART.D_ITEM;
DROP TABLE IF EXISTS MART.D_CUSTOMER;
DROP TABLE IF EXISTS MART.D_CITY;
DROP TABLE IF EXISTS MART.LOAD_HISTORY;

-- ----------------------------
-- Dimension Table: Customer
-- Stores customer master data
-- ----------------------------
CREATE TABLE MART.D_CUSTOMER (
    ID SERIAL NOT NULL,
    CUSTOMER_ID INT PRIMARY KEY,
    FIRST_NAME VARCHAR(15),
    LAST_NAME VARCHAR(15),
    CITY_ID INT
);
CREATE INDEX D_CUST1 ON MART.D_CUSTOMER (CUSTOMER_ID);

-- ----------------------------
-- Dimension Table: City
-- Stores city reference data
-- ----------------------------
CREATE TABLE MART.D_CITY (
    ID SERIAL PRIMARY KEY,
    CITY_ID INT,
    CITY_NAME VARCHAR(50)
);
CREATE INDEX D_CITY1 ON MART.D_CITY (CITY_ID);

-- ----------------------------
-- Dimension Table: Item
-- Stores item master data
-- ----------------------------
CREATE TABLE MART.D_ITEM (
    ID SERIAL NOT NULL,
    ITEM_ID INT PRIMARY KEY,
    ITEM_NAME VARCHAR(50),
    CATEGORY_ID INT NOT NULL
);
CREATE INDEX D_ITEM1 ON MART.D_ITEM (ITEM_ID);

-- ----------------------------
-- Dimension Table: Category
-- Stores product categories
-- ----------------------------
CREATE TABLE MART.D_CATEGORY (
    ID SERIAL PRIMARY KEY,
    CATEGORY_ID INT NOT NULL,
    CATEGORY_NAME VARCHAR(50)
);
CREATE INDEX D_CATEGORY1 ON MART.D_CATEGORY (CATEGORY_ID);

-- ----------------------------
-- Dimension Table: Calendar
-- Stores date reference data
-- ----------------------------
CREATE TABLE MART.D_CALENDAR (
    DATE_ID INT PRIMARY KEY,
    FACT_DATE DATE NOT NULL,
    DAY_NUM INT,
    MONTH_NUM INT,
    MONTH_NAME VARCHAR(8),
    YEAR_NUM INT
);
CREATE INDEX D_CALENDAR1 ON MART.D_CALENDAR (YEAR_NUM);

-- ----------------------------
-- Fact Table: Activity
-- Stores aggregated user activity metrics
-- ----------------------------
CREATE TABLE MART.F_ACTIVITY (
    ACTIVITY_ID INT NOT NULL,
    DATE_ID INT NOT NULL,
    CLICK_NUMBER INT,
    PRIMARY KEY (ACTIVITY_ID, DATE_ID),
    FOREIGN KEY (DATE_ID) REFERENCES MART.D_CALENDAR(DATE_ID) ON UPDATE CASCADE
);
CREATE INDEX F_ACTIVITY1 ON MART.F_ACTIVITY (DATE_ID);
CREATE INDEX F_ACTIVITY2 ON MART.F_ACTIVITY (ACTIVITY_ID);

-- ----------------------------
-- Fact Table: Daily Sales
-- Stores aggregated daily sales per item and customer
-- ----------------------------
CREATE TABLE MART.F_DAILY_SALES (
    DATE_ID INT NOT NULL,
    ITEM_ID INT NOT NULL,
    CUSTOMER_ID INT NOT NULL,
    PRICE DECIMAL(10,2),
    QUANTITY BIGINT,
    PAYMENT_AMOUNT DECIMAL(10,2),
    PRIMARY KEY (DATE_ID, ITEM_ID, CUSTOMER_ID),
    FOREIGN KEY (DATE_ID) REFERENCES MART.D_CALENDAR(DATE_ID) ON UPDATE CASCADE,
    FOREIGN KEY (ITEM_ID) REFERENCES MART.D_ITEM(ITEM_ID) ON UPDATE CASCADE,
    FOREIGN KEY (CUSTOMER_ID) REFERENCES MART.D_CUSTOMER(CUSTOMER_ID) ON UPDATE CASCADE
);
CREATE INDEX F_DS1 ON MART.F_DAILY_SALES (DATE_ID);
CREATE INDEX F_DS2 ON MART.F_DAILY_SALES (ITEM_ID);
CREATE INDEX F_DS3 ON MART.F_DAILY_SALES (CUSTOMER_ID);

-- ----------------------------
-- Fact Table: Research
-- Stores aggregated customer research metrics
-- ----------------------------
CREATE TABLE MART.F_RESEARCH (
    DATE_ID INT NOT NULL,
    CATEGORY_ID INT NOT NULL,
    GEO_ID INT NOT NULL,
    CUSTOMER_ID INT NOT NULL,
    ITEM_ID INT NOT NULL,
    QUANTITY BIGINT,
    AMOUNT DECIMAL(10,2),
    PRIMARY KEY (DATE_ID, CATEGORY_ID, GEO_ID),
    FOREIGN KEY (DATE_ID) REFERENCES MART.D_CALENDAR(DATE_ID) ON UPDATE CASCADE,
    FOREIGN KEY (ITEM_ID) REFERENCES MART.D_ITEM(ITEM_ID) ON UPDATE CASCADE,
    FOREIGN KEY (CUSTOMER_ID) REFERENCES MART.D_CUSTOMER(CUSTOMER_ID) ON UPDATE CASCADE
);
CREATE INDEX F_R1 ON MART.F_RESEARCH (DATE_ID);
CREATE INDEX F_R2 ON MART.F_RESEARCH (ITEM_ID);
CREATE INDEX F_R3 ON MART.F_RESEARCH (CUSTOMER_ID);

-- ----------------------------
-- Load History Table
-- Tracks ETL batch loads for each target table
-- ----------------------------
CREATE TABLE MART.LOAD_HISTORY (
    BATCH_ID BIGINT,
    TARGET_TABLE VARCHAR(50),
    START_LOAD TIMESTAMP,
    FINISH_LOAD TIMESTAMP,
    PRIMARY KEY (BATCH_ID, TARGET_TABLE)
);