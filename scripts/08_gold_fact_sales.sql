/***********************************************************************************
Script Name: 08_gold_fact_sales.sql
Description: Creates the Sales Fact view in the Gold Layer
Author: Salah
Created Date: 2025
Project: SQL Data Warehouse - Medallion Architecture

Purpose:
    This script creates the sales fact table by integrating transactional sales
    data with customer and product dimensions. It forms the central fact table
    in the star schema for sales analytics and reporting.

View: gold.fact_sales

Data Integration:
    - Base sales transactions from CRM (silver.crm_sales_details)
    - Linked to customer dimension (gold.dim_customer)
    - Linked to product dimension (gold.dim_product)

Key Features:
    - Central fact table in star schema
    - Contains sales metrics (quantity, price, sales amount)
    - Foreign keys to customer and product dimensions
    - Includes multiple date dimensions (order, shipping, due dates)
    - LEFT JOINs ensure all sales included even if dimension lookup fails

Fact Table Type: Transaction Grain
    - Each row represents one line item in a sales order
    - Grain: One row per product per order

Columns:
    Identifiers:
        - order_number      : Sales order number (business key)
        - product_key       : Foreign key to dim_product
        - customer_key      : Foreign key to dim_customer
    
    Date Dimensions:
        - order_date        : Date when order was placed
        - shipping_date     : Date when order was shipped
        - due_date          : Date when order is due
    
    Measures (Metrics):
        - sales_amount      : Total sales amount for the line item
        - quantity          : Quantity of products sold
        - price             : Unit price per product

Usage Examples:
    -- Total sales by customer
    SELECT 
        c.first_name, c.last_name,
        SUM(f.sales_amount) as total_sales
    FROM gold.fact_sales f
    JOIN gold.dim_customer c ON f.customer_key = c.customer_key
    GROUP BY c.first_name, c.last_name;

    -- Sales by product category
    SELECT 
        p.categorie_, p.line,
        SUM(f.sales_amount) as total_sales,
        SUM(f.quantity) as total_quantity
    FROM gold.fact_sales f
    JOIN gold.dim_product p ON f.product_key = p.product_key
    GROUP BY p.categorie_, p.line;

    -- Monthly sales trend
    SELECT 
        YEAR(order_date) as year,
        MONTH(order_date) as month,
        SUM(sales_amount) as monthly_sales
    FROM gold.fact_sales
    GROUP BY YEAR(order_date), MONTH(order_date)
    ORDER BY year, month;

Dependencies:
    - silver.crm_sales_details
    - gold.dim_customer
    - gold.dim_product

Notes:
    - View automatically reflects latest data from silver layer
    - No data stored physically (virtual view)
    - Join keys: sls_cust_id = customer_id, sls_prd_key = serial_number
    - Designed for OLAP queries and aggregations
    - Optimized for star schema query patterns
***********************************************************************************/

if OBJECT_ID('gold.fact_sales','V') is not null
    drop view gold.fact_sales
go

create view gold.fact_sales as
select 
    sd.sls_ord_num AS order_number,
    dp.product_key as product_key,
    dc.customer_key AS customer_key,
    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt AS shipping_date,
    sd.sls_due_dt AS due_date,
    sd.sls_sales AS sales_amount,
    sd.sls_quantity AS quantity,
    sd.sls_price as price
from silver.crm_sales_details as sd
left join gold.dim_customer as dc 
    on sd.sls_cust_id = dc.customer_id
left join gold.dim_product as dp 
    on sd.sls_prd_key = dp.serial_number
