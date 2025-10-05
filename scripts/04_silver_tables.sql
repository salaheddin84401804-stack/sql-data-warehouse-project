/***********************************************************************************
Script Name: 04_silver_tables.sql
Description: Creates all Silver Layer tables for cleansed and validated data
Author: Salah
Created Date: 2025
Project: SQL Data Warehouse - Medallion Architecture

Purpose:
    This script creates the Silver Layer tables that store cleansed, validated,
    and standardized data. These tables have the same structure as Bronze Layer
    but include audit columns for tracking data lineage and updates.

Tables Created:
    CRM Source System:
        - silver.crm_cust_info      : Cleansed customer master data
        - silver.crm_prd_info       : Cleansed product catalog (with cat_key added)
        - silver.crm_sales_details  : Cleansed sales transactions
    
    ERP Source System:
        - silver.erp_cust_az12      : Cleansed customer demographics
        - silver.erp_loc_a101       : Cleansed customer locations
        - silver.erp_px_cat_g1v2    : Cleansed product categories

Key Differences from Bronze:
    - Added 'date_update' column with default GETDATE() for audit tracking
    - crm_prd_info includes 'cat_key' column for category linkage
    - Data types remain consistent for transformation processing
    - Ready to receive validated and standardized data

Usage:
    Run this script after 02_bronze_tables.sql
    These tables will be populated by the silver loading procedure

Notes:
    - All tables use DROP IF EXISTS for safe re-execution
    - date_update column automatically captures load timestamp
    - Tables serve as the trusted source for downstream gold layer
***********************************************************************************/

-- =====================================================================================
-- CRM SOURCE SYSTEM TABLES (SILVER LAYER)
-- =====================================================================================

-- ---------------------------------------------------------------------------------
-- Table: silver.crm_cust_info
-- Description: Cleansed customer master data with standardized values
-- ---------------------------------------------------------------------------------
IF OBJECT_ID('silver.crm_cust_info','U') IS NOT NULL
    DROP TABLE silver.crm_cust_info;
GO

CREATE TABLE silver.crm_cust_info
(
    cst_id              INT,
    cst_key             NVARCHAR(50),
    cst_firstname       NVARCHAR(50),
    cst_lastname        NVARCHAR(50),
    cst_marital_status  NVARCHAR(50),
    cst_gndr            NVARCHAR(50),
    cst_create_date     DATE,
    date_update         DATETIME DEFAULT GETDATE()
);
GO

-- ---------------------------------------------------------------------------------
-- Table: silver.crm_prd_info
-- Description: Cleansed product catalog with category key separation
-- ---------------------------------------------------------------------------------
IF OBJECT_ID('silver.crm_prd_info','U') IS NOT NULL
    DROP TABLE silver.crm_prd_info;
GO

CREATE TABLE silver.crm_prd_info
(
    prd_id          INT,
    cat_key         NVARCHAR(50),
    prd_key         NVARCHAR(50),
    prd_nm          NVARCHAR(50),
    prd_cost        INT,
    prd_line        NVARCHAR(50),
    prd_start_dt    DATE,
    prd_end_dt      DATE,
    date_update     DATETIME DEFAULT GETDATE()
);
GO

-- ---------------------------------------------------------------------------------
-- Table: silver.crm_sales_details
-- Description: Cleansed sales transactions with corrected date formats
-- ---------------------------------------------------------------------------------
IF OBJECT_ID('silver.crm_sales_details','U') IS NOT NULL
    DROP TABLE silver.crm_sales_details;
GO

CREATE TABLE silver.crm_sales_details
(
    sls_ord_num     NVARCHAR(50),
    sls_prd_key     NVARCHAR(50),
    sls_cust_id     INT,
    sls_order_dt    DATE,
    sls_ship_dt     DATE,
    sls_due_dt      DATE,
    sls_sales       INT,
    sls_quantity    INT,
    sls_price       INT,
    date_update     DATETIME DEFAULT GETDATE()
);
GO

-- =====================================================================================
-- ERP SOURCE SYSTEM TABLES (SILVER LAYER)
-- =====================================================================================

-- ---------------------------------------------------------------------------------
-- Table: silver.erp_cust_az12
-- Description: Cleansed customer demographics with standardized gender values
-- ---------------------------------------------------------------------------------
IF OBJECT_ID('silver.erp_cust_az12','U') IS NOT NULL
    DROP TABLE silver.erp_cust_az12;
GO

CREATE TABLE silver.erp_cust_az12
(
    cid             NVARCHAR(50),
    bdate           DATE,
    gen             NVARCHAR(50),
    date_update     DATETIME DEFAULT GETDATE()
);
GO

-- ---------------------------------------------------------------------------------
-- Table: silver.erp_loc_a101
-- Description: Cleansed customer location with standardized country names
-- ---------------------------------------------------------------------------------
IF OBJECT_ID('silver.erp_loc_a101','U') IS NOT NULL
    DROP TABLE silver.erp_loc_a101;
GO

CREATE TABLE silver.erp_loc_a101
(
    cid             NVARCHAR(50),
    cntry           NVARCHAR(50),
    date_update     DATETIME DEFAULT GETDATE()
);
GO

-- ---------------------------------------------------------------------------------
-- Table: silver.erp_px_cat_g1v2
-- Description: Cleansed product category and maintenance information
-- ---------------------------------------------------------------------------------
IF OBJECT_ID('silver.erp_px_cat_g1v2','U') IS NOT NULL
    DROP TABLE silver.erp_px_cat_g1v2;
GO

CREATE TABLE silver.erp_px_cat_g1v2
(
    id              NVARCHAR(50),
    cat             NVARCHAR(50),
    subcat          NVARCHAR(50),
    maintenance     NVARCHAR(50),
    date_update     DATETIME DEFAULT GETDATE()
);
GO
