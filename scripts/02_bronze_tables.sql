/***********************************************************************************
Script Name: 02_bronze_tables.sql
Description: Creates all Bronze Layer tables for raw data ingestion
Author: Salah
Created Date: 2025
Project: SQL Data Warehouse - Medallion Architecture

Purpose:
    This script creates the Bronze Layer tables that store raw, unprocessed data
    from source systems (CRM and ERP). No transformations or validations are
    applied at this layer - data is stored exactly as received.

Tables Created:
    CRM Source System:
        - bronz.crm_cust_info      : Customer master data
        - bronz.crm_prd_info       : Product catalog information
        - bronz.crm_sales_details  : Sales transaction details
    
    ERP Source System:
        - bronz.erp_cust_az12      : Customer demographics data
        - bronz.erp_loc_a101       : Customer location information
        - bronz.erp_px_cat_g1v2    : Product category and maintenance info

Usage:
    Run this script after 01_database_setup.sql
    These tables will be populated by the bronze loading procedure

Notes:
    - All tables use DROP IF EXISTS for safe re-execution
    - No constraints or indexes applied (raw landing zone)
    - Data types match source CSV file formats
    - Tables are truncated and reloaded during ETL process
***********************************************************************************/

-- =====================================================================================
-- CRM SOURCE SYSTEM TABLES
-- =====================================================================================

-- ---------------------------------------------------------------------------------
-- Table: bronz.crm_cust_info
-- Description: Customer master data from CRM system
-- ---------------------------------------------------------------------------------
if OBJECT_ID('bronz.crm_cust_info','U') is not null
    drop table bronz.crm_cust_info
go

create table bronz.crm_cust_info
(
    cst_id              int,
    cst_key             nvarchar(50),
    cst_firstname       nvarchar(50),
    cst_lastname        nvarchar(50),
    cst_marital_status  nvarchar(50),
    cst_gndr            nvarchar(50),
    cst_create_date     date
);
go

-- ---------------------------------------------------------------------------------
-- Table: bronz.crm_prd_info
-- Description: Product catalog and information from CRM system
-- ---------------------------------------------------------------------------------
if OBJECT_ID('bronz.crm_prd_info','U') is not null
    drop table bronz.crm_prd_info
go

create table bronz.crm_prd_info
(
    prd_id          int,
    prd_key         nvarchar(50),
    prd_nm          nvarchar(50),
    prd_cost        int,
    prd_line        nvarchar(50),
    prd_start_dt    date,
    prd_end_dt      date
);
go

-- ---------------------------------------------------------------------------------
-- Table: bronz.crm_sales_details
-- Description: Sales transaction details from CRM system
-- ---------------------------------------------------------------------------------
if OBJECT_ID('bronz.crm_sales_details','U') is not null
    drop table bronz.crm_sales_details
go

create table bronz.crm_sales_details
(
    sls_ord_num     nvarchar(50),
    sls_prd_key     nvarchar(50),
    sls_cust_id     int,
    sls_order_dt    int,
    sls_ship_dt     int,
    sls_due_dt      int,
    sls_sales       int,
    sls_quantity    int,
    sls_price       int
);
go

-- =====================================================================================
-- ERP SOURCE SYSTEM TABLES
-- =====================================================================================

-- ---------------------------------------------------------------------------------
-- Table: bronz.erp_cust_az12
-- Description: Customer demographics data from ERP system
-- ---------------------------------------------------------------------------------
if OBJECT_ID('bronz.erp_cust_az12','U') is not null
    drop table bronz.erp_cust_az12
go

create table bronz.erp_cust_az12
(
    cid     nvarchar(50),
    bdate   date,
    gen     nvarchar(50)
);

-- ---------------------------------------------------------------------------------
-- Table: bronz.erp_loc_a101
-- Description: Customer location information from ERP system
-- ---------------------------------------------------------------------------------
if OBJECT_ID('bronz.erp_loc_a101','U') is not null
    drop table bronz.erp_loc_a101
go

create table bronz.erp_loc_a101
(
    cid     nvarchar(50),
    cntry   nvarchar(50)
);

-- ---------------------------------------------------------------------------------
-- Table: bronz.erp_px_cat_g1v2
-- Description: Product category and maintenance data from ERP system
-- ---------------------------------------------------------------------------------
if OBJECT_ID('bronz.erp_px_cat_g1v2','U') is not null
    drop table bronz.erp_px_cat_g1v2
go

create table bronz.erp_px_cat_g1v2
(
    id              nvarchar(50),
    cat             nvarchar(50),
    subcat          nvarchar(50),
    maintenance     nvarchar(50)
);
go
