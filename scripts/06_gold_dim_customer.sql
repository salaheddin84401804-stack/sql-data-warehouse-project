/***********************************************************************************
Script Name: 06_gold_dim_customer.sql
Description: Creates the Customer Dimension view in the Gold Layer
Author: Salah
Created Date: 2025
Project: SQL Data Warehouse - Medallion Architecture

Purpose:
    This script creates the unified customer dimension by integrating customer
    data from multiple source systems (CRM and ERP). It provides a complete
    360-degree view of customer information for analytics and reporting.

View: gold.dim_customer

Data Integration:
    - Base data from CRM customer info (silver.crm_cust_info)
    - Enhanced with demographics from ERP (silver.erp_cust_az12)
    - Enriched with location data from ERP (silver.erp_loc_a101)

Key Features:
    - Generates surrogate key (customer_key) using ROW_NUMBER()
    - Merges gender information from both CRM and ERP (ERP takes precedence if CRM is N\A)
    - Provides complete customer profile including demographics and location
    - LEFT JOINs ensure all CRM customers are included even without ERP data

Columns:
    - customer_key        : Surrogate key for dimension (auto-generated)
    - customer_id         : Business key from CRM system
    - customer_number     : Customer reference number
    - first_name          : Customer first name
    - last_name           : Customer last name
    - gender              : Standardized gender (merged from CRM and ERP)
    - birth_date          : Date of birth from ERP
    - marital_status      : Marital status from CRM
    - contry              : Customer country from ERP
    - create_date         : Customer creation date in CRM

Usage:
    SELECT * FROM gold.dim_customer;
    SELECT * FROM gold.dim_customer WHERE contry = 'United States';
    SELECT * FROM gold.dim_customer WHERE gender = 'Female';

Dependencies:
    - silver.crm_cust_info
    - silver.erp_cust_az12
    - silver.erp_loc_a101

Notes:
    - View automatically reflects latest data from silver layer
    - No data stored physically (virtual view)
    - Surrogate keys regenerated on each query
    - Join key: cst_key = cid (customer identifier across systems)
***********************************************************************************/

if OBJECT_ID('gold.dim_customer','V')is not null
    drop view gold.dim_customer;
go

create view gold.dim_customer as
select 
    ROW_NUMBER() over(order by ci.cst_id) as customer_key,
    ci.cst_id as customer_id,
    ci.cst_key as customer_number,
    ci.cst_firstname as first_name,
    ci.cst_lastname as last_name,
    case 
        when ci.cst_gndr = 'N\A' or ci.cst_gndr is null then ISNULL(ca.gen,'N\A')
        else ci.cst_gndr 
    end gender,
    ca.bdate as birth_date,
    ci.cst_marital_status as marital_status,
    la.cntry as contry,
    ci.cst_create_date as create_date
from silver.crm_cust_info as ci
left join silver.erp_cust_az12 as ca 
    on ci.cst_key = ca.cid
left join silver.erp_loc_a101 as la 
    on ci.cst_key = la.cid
