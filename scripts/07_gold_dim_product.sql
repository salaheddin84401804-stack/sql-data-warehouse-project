/***********************************************************************************
Script Name: 07_gold_dim_product.sql
Description: Creates the Product Dimension view in the Gold Layer
Author: Salah
Created Date: 2025
Project: SQL Data Warehouse - Medallion Architecture

Purpose:
    This script creates the product dimension by integrating product catalog
    data from CRM with product category information from ERP. It provides a
    complete product hierarchy for analytics and reporting.

View: gold.dim_product

Data Integration:
    - Base data from CRM product info (silver.crm_prd_info)
    - Enhanced with category hierarchy from ERP (silver.erp_px_cat_g1v2)
    - Includes product lifecycle dates (start and end dates)

Key Features:
    - Generates surrogate key (product_key) using ROW_NUMBER()
    - Provides complete product hierarchy (category → subcategory)
    - Includes product cost and line information
    - Supports product lifecycle tracking with start/end dates
    - LEFT JOIN ensures all products included even without category data

Columns:
    - product_key        : Surrogate key for dimension (auto-generated)
    - product_id         : Business key from CRM system
    - categorie_key      : Category identifier (links to ERP categories)
    - categorie_         : Product category name
    - sub_categorie      : Product subcategory name
    - serial_number      : Product serial/reference number
    - coste              : Product cost
    - line               : Product line (Mountain, Road, Touring, Other Sales)
    - start_date         : Product availability start date
    - end_date           : Product availability end date (NULL if still active)
    - maintenance        : Maintenance information from ERP

Usage:
    SELECT * FROM gold.dim_product;
    SELECT * FROM gold.dim_product WHERE line = 'Mountain';
    SELECT * FROM gold.dim_product WHERE categorie_ = 'Bikes';
    SELECT * FROM gold.dim_product WHERE end_date IS NULL; -- Active products

Dependencies:
    - silver.crm_prd_info
    - silver.erp_px_cat_g1v2

Notes:
    - View automatically reflects latest data from silver layer
    - No data stored physically (virtual view)
    - Surrogate keys regenerated on each query
    - Join key: cat_key = id (category identifier across systems)
    - Products without end_date are currently active
***********************************************************************************/

if OBJECT_ID('gold.dim_product','V') is not null
    drop view gold.dim_product
go

create view gold.dim_product as
select 
    ROW_NUMBER() over(order by pf.prd_id) as product_key,
    pf.prd_id as product_id,
    pf.cat_key as categorie_key,
    pc.cat as categorie_,
    pc.subcat as sub_categorie,
    pf.prd_key as serial_number,
    pf.prd_cost as coste,
    pf.prd_line as line,
    pf.prd_start_dt as start_date,
    pf.prd_end_dt as end_date,
    pc.maintenance as maintenance
from silver.crm_prd_info as pf
left join silver.erp_px_cat_g1v2 as pc 
    on pf.cat_key = pc.id
