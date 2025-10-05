# SQL Data Warehouse Project

## 📋 Overview

This project demonstrates a complete **data warehouse implementation** using SQL Server and the **Medallion Architecture** pattern. I built an end-to-end ETL pipeline that integrates data from multiple source systems (CRM and ERP) and transforms it into a business-ready dimensional model for analytics.

## 🎯 What I Built

I created a three-layer data warehouse that:
- Ingests raw data from CSV files (CRM and ERP systems)
- Cleanses and transforms the data with quality rules
- Provides a star schema dimensional model for business intelligence

## 🏗️ Architecture - Medallion Pattern

I implemented the medallion architecture with three distinct layers:

### Bronze Layer (Raw Zone)
- Loaded raw data from 6 CSV source files using BULK INSERT
- No transformations applied - data stored as-is
- Includes CRM data (customers, products, sales) and ERP data (demographics, locations, categories)

### Silver Layer (Cleansed Zone)
- Applied data quality rules and transformations
- Standardized codes and values (gender, marital status, countries)
- Fixed data types and date formats
- Removed duplicates and handled null values
- Added audit timestamp columns

### Gold Layer (Business Zone)
- Created dimensional model (star schema)
- Built 2 dimension views and 1 fact view
- Ready for BI tools and reporting

## 📊 Dimensional Model

I designed a star schema with:

**Dimensions:**
- `dim_customer` - Customer demographics and profile (unified from CRM + ERP)
- `dim_product` - Product catalog with category hierarchy

**Fact:**
- `fact_sales` - Sales transactions with metrics (quantity, price, sales amount)

## 🔧 What I Did

### 1. Database Setup
- Created database and three schemas (bronz, silver, gold)
- Organized objects by layer for clear separation

### 2. Data Ingestion (Bronze)
- Built tables to receive raw CSV data
- Created stored procedure `bronz.load_bronz_layer` with:
  - BULK INSERT from CSV files
  - Error handling and logging
  - Execution time tracking

### 3. Data Transformation (Silver)
- Implemented business rules for data cleansing:
  - Expanded abbreviations (M → Male, S → Single)
  - Standardized country codes (US/USA → United States)
  - Converted date formats (integer YYYYMMDD → DATE)
  - Recalculated sales amounts (quantity × price)
  - Removed duplicates using ROW_NUMBER()
  - Validated and cleaned customer IDs
- Created stored procedure `silver.load_silver_layer` with full ETL logic

### 4. Dimensional Model (Gold)
- Created customer dimension by joining CRM and ERP customer data
- Built product dimension with category information
- Developed sales fact table with foreign keys to dimensions
- Used views for flexibility and ease of maintenance

## 🚀 Technologies & Techniques

**SQL Server Features Used:**
- T-SQL stored procedures
- BULK INSERT for efficient data loading
- Window functions (ROW_NUMBER, LEAD)
- Views for dimensional model
- TRY-CATCH for error handling
- CASE statements for data standardization

**ETL Patterns:**
- Truncate and load (full refresh)
- Data quality validation
- Surrogate key generation
- Slowly changing dimensions (Type 1)
- Audit timestamps

## 📁 Project Structure

```
├── 01_database_setup.sql          # Database and schema creation
├── 02_bronze_tables.sql           # Bronze layer tables
├── 03_bronze_load_proc.sql        # Bronze ETL procedure
├── 04_silver_tables.sql           # Silver layer tables
├── 05_silver_load_proc.sql        # Silver ETL procedure
├── 06_gold_dim_customer.sql       # Customer dimension
├── 07_gold_dim_product.sql        # Product dimension
├── 08_gold_fact_sales.sql         # Sales fact table
└── README.md
```

## 🔄 ETL Pipeline

The complete pipeline runs in sequence:

```sql
-- Step 1: Load raw data
EXEC bronz.load_bronz_layer;

-- Step 2: Transform and cleanse
EXEC silver.load_silver_layer;

-- Step 3: Query dimensional model
SELECT * FROM gold.fact_sales;
SELECT * FROM gold.dim_customer;
SELECT * FROM gold.dim_product;
```

## 💡 Key Features

✅ Automated ETL with stored procedures  
✅ Comprehensive error handling and logging  
✅ Data quality rules and validation  
✅ Performance tracking (execution times)  
✅ Audit trail with timestamps  
✅ Modular and maintainable code  
✅ Star schema for analytics  

## 📈 Sample Analysis Query

```sql
-- Sales by product category and country
SELECT 
    p.categorie_,
    c.contry,
    COUNT(DISTINCT f.order_number) AS total_orders,
    SUM(f.sales_amount) AS total_sales
FROM gold.fact_sales f
JOIN gold.dim_customer c ON f.customer_key = c.customer_key
JOIN gold.dim_product p ON f.product_key = p.product_key
GROUP BY p.categorie_, c.contry
ORDER BY total_sales DESC;
```

## 🎓 What I Learned

- Designing and implementing medallion architecture
- Building ETL pipelines with SQL Server
- Data quality and cleansing techniques
- Dimensional modeling (star schema)
- Performance optimization with BULK INSERT
- Error handling and logging in T-SQL

## 📝 Notes

- File paths in the bronze loading procedure need to be updated for your environment
- The pipeline uses a full refresh pattern (truncate and load)
- All layers are independent and can be rebuilt from source

---

**Project Type:** Data Warehouse | **Pattern:** Medallion Architecture | **Tool:** SQL Server
