/***********************************************************************************
Script Name: 05_silver_load_proc.sql
Description: Stored procedure to transform and load cleansed data into Silver Layer
Author: Salah
Created Date: 2025
Project: SQL Data Warehouse - Medallion Architecture

Purpose:
    This procedure orchestrates the transformation and cleansing of data from
    Bronze to Silver Layer. It applies business rules, data quality validations,
    standardizations, and type conversions to create a trusted data layer.

Procedure: silver.load_silver_layer
    - Reads raw data from Bronze Layer tables
    - Applies data cleansing and transformation rules
    - Loads validated data into Silver Layer tables
    - Tracks load duration for each table
    - Provides detailed console logging
    - Handles errors gracefully with TRY-CATCH

Key Transformations Applied:
    CRM Customer:
        - Trim whitespace from names
        - Expand marital status codes (S→Single, M→Married)
        - Standardize gender values (F→Female, M→Male)
        - Remove duplicates (keep latest by create date)
        - Filter out null customer IDs
    
    CRM Product:
        - Parse product key into category key and serial number
        - Expand product line codes (M→Mountain, R→Road, T→Touring, S→Other Sales)
        - Handle null costs (default to 0)
        - Calculate end dates using LEAD window function
        - Trim product names
    
    CRM Sales:
        - Convert integer dates (YYYYMMDD) to DATE type
        - Validate date format (must be 8 digits)
        - Recalculate sales amounts (quantity × price)
        - Fix invalid or null prices
        - Ensure data consistency
    
    ERP Customer:
        - Clean customer IDs (remove 'NAS' prefix)
        - Validate birth dates (reject dates after 2010)
        - Standardize gender values and variations
    
    ERP Location:
        - Clean customer IDs (remove hyphens)
        - Expand country codes (DE→Germany, US/USA→United States)
        - Handle null/empty countries (default to N\A)
    
    ERP Product Category:
        - Standardize ID format (underscore to hyphen)

Usage:
    EXEC silver.load_silver_layer;

Dependencies:
    - Requires Bronze Layer tables to be populated first
    - Run after bronz.load_bronz_layer procedure

Notes:
    - Full refresh pattern (truncate and load)
    - Data quality rules enforce business standards
    - Audit timestamp automatically captured in date_update column
    - Performance optimized with window functions
***********************************************************************************/

CREATE OR ALTER PROCEDURE silver.load_silver_layer
AS
BEGIN
---------------------------------------------------------------------------------------------------
    DECLARE 
        @start_time   DATETIME,
        @end_time     DATETIME,
        @P_start_time DATETIME,
        @P_end_time   DATETIME;
---------------------------------------------------------------------------------------------------
    BEGIN TRY
---------------------------------------------------------------------------------------------------
        SET @P_start_time = GETDATE();
---------------------------------------------------------------------------------------------------
        PRINT '=================================================';
        PRINT 'Loading Silver Layer';
        PRINT '=================================================';

---------------------------------------------------------------------------------------------------
        PRINT '-------------------------------------------------';
        PRINT 'Loading CRM Tables';
        PRINT '-------------------------------------------------';

---------------------------------------------------------------------------------------------------
        ---------------------------
        -- 1. CRM Customer Table
        ---------------------------
        SET @start_time = GETDATE();
        PRINT '1-Truncating Table: silver.crm_cust_info';
        TRUNCATE TABLE silver.crm_cust_info;

        PRINT '1-Inserting Data Into: silver.crm_cust_info';
        INSERT INTO silver.crm_cust_info  
        (
            cst_id,
            cst_key,
            cst_firstname,
            cst_lastname,
            cst_marital_status,
            cst_gndr,
            cst_create_date
        )
        SELECT  
            cst_id,
            cst_key,
            TRIM(cst_firstname) AS cst_firstname,
            TRIM(cst_lastname) AS cst_lastname,
            CASE
                WHEN cst_marital_status = 'S' THEN 'Singel'
                WHEN cst_marital_status = 'M' THEN 'Maried'
                ELSE 'N\A'
            END AS cst_marital_status,
            CASE 
                WHEN cst_gndr = 'F' THEN 'Female'
                WHEN cst_gndr = 'M' THEN 'Male'
                ELSE 'N\A'
            END AS cst_gndr,
            cst_create_date
        FROM (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag
            FROM bronz.crm_cust_info
            WHERE cst_id IS NOT NULL
        ) t
        WHERE flag = 1;

        SET @end_time = GETDATE();
        PRINT 'Load Duration CRM_CUST_INFO: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
---------------------------------------------------------------------------------------------------
        ---------------------------
        -- 2. CRM Product Table
        ---------------------------
        SET @start_time = GETDATE();
        PRINT '2-Truncating Table: silver.crm_prd_info';
        TRUNCATE TABLE silver.crm_prd_info;

        PRINT '2-Inserting Data Into: silver.crm_prd_info';
        INSERT INTO silver.crm_prd_info
        (
            prd_id,
            cat_key,
            prd_key,
            prd_nm,
            prd_cost,
            prd_line,
            prd_start_dt,
            prd_end_dt
        )
        SELECT 
            prd_id,
            SUBSTRING(prd_key,1,5) AS cat_id,
            SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
            TRIM(prd_nm) AS prd_nm,
            CASE
                WHEN prd_cost IS NULL THEN 0
                ELSE prd_cost
            END AS prd_cost,
            CASE 
                WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
                WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
                WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
                WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
                ELSE 'n/a'
            END AS prd_line,
            prd_start_dt,
            DATEADD(DAY,-1,LEAD(prd_start_dt) OVER(PARTITION BY prd_nm ORDER BY prd_start_dt)) AS prd_end_dt
        FROM bronz.crm_prd_info;

        SET @end_time = GETDATE();
        PRINT 'Load Duration CRM_PRD_INFO: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
---------------------------------------------------------------------------------------------------
        ---------------------------
        -- 3. CRM Sales Details Table
        ---------------------------
        SET @start_time = GETDATE();
        PRINT '3-Truncating Table: silver.crm_sales_details';
        TRUNCATE TABLE silver.crm_sales_details;

        PRINT '3-Inserting Data Into: silver.crm_sales_details';
        INSERT INTO silver.crm_sales_details
        (
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            sls_order_dt,
            sls_ship_dt,
            sls_due_dt,
            sls_sales,
            sls_quantity,
            sls_price
        )
        SELECT 
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            CASE WHEN LEN(sls_order_dt) != 8 THEN NULL ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE) END AS sls_order_dt,
            CASE WHEN LEN(sls_ship_dt) != 8 THEN NULL ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE) END AS sls_ship_dt,
            CASE WHEN LEN(sls_due_dt) != 8 THEN NULL ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE) END AS sls_due_dt,
            CASE 
                WHEN sls_sales != ABS(sls_quantity)*ABS(sls_price) OR sls_sales IS NULL THEN ABS(sls_quantity)*ABS(sls_price)
                ELSE sls_sales
            END AS sls_sales,
            sls_quantity,
            CASE 
                WHEN sls_price <= 0 OR sls_price IS NULL THEN sls_sales / sls_quantity
                ELSE sls_price
            END AS sls_price
        FROM bronz.crm_sales_details;

        SET @end_time = GETDATE();
        PRINT 'Load Duration CRM_SALES_DETAILS: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
---------------------------------------------------------------------------------------------------
        PRINT '-------------------------------------------------';
        PRINT 'Loading ERP Tables';
        PRINT '-------------------------------------------------';
---------------------------------------------------------------------------------------------------
        ---------------------------
        -- 4. ERP Customer Table
        ---------------------------
        SET @start_time = GETDATE();
        PRINT '4-Truncating Table: silver.erp_cust_az12';
        TRUNCATE TABLE silver.erp_cust_az12;

        PRINT '4-Inserting Data Into: silver.erp_cust_az12';
        INSERT INTO silver.erp_cust_az12
        (
            cid,
            bdate,
            gen
        )
        SELECT 
            REPLACE(cid,'NAS','') AS cid,
            CASE WHEN bdate > '2010-01-01' THEN NULL ELSE bdate END AS bdate,
            CASE 
                WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
                WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
                ELSE 'N\A'
            END AS gen
        FROM bronz.erp_cust_az12;

        SET @end_time = GETDATE();
        PRINT 'Load Duration ERP_CUST_AZ12: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
---------------------------------------------------------------------------------------------------
        ---------------------------
        -- 5. ERP Location Table
        ---------------------------
        SET @start_time = GETDATE();
        PRINT '5-Truncating Table: silver.erp_loc_a101';
        TRUNCATE TABLE silver.erp_loc_a101;

        PRINT '5-Inserting Data Into: silver.erp_loc_a101';
        INSERT INTO silver.erp_loc_a101
        (
            cid,
            cntry
        )
        SELECT 
            REPLACE(cid,'-','') AS cid,
            CASE 
                WHEN UPPER(TRIM(cntry)) = 'DE' THEN 'Germany'
                WHEN UPPER(TRIM(cntry)) IN ('US','USA') THEN 'United States'
                WHEN UPPER(TRIM(cntry)) IS NULL OR UPPER(TRIM(cntry)) = '' THEN 'N\A'
                ELSE cntry
            END AS cntry
        FROM bronz.erp_loc_a101;

        SET @end_time = GETDATE();
        PRINT 'Load Duration ERP_LOC_A101: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
---------------------------------------------------------------------------------------------------
        ---------------------------
        -- 6. ERP Price Category Table
        ---------------------------
        SET @start_time = GETDATE();
        PRINT '6-Truncating Table: silver.erp_px_cat_g1v2';
        TRUNCATE TABLE silver.erp_px_cat_g1v2;

        PRINT '6-Inserting Data Into: silver.erp_px_cat_g1v2';
        INSERT INTO silver.erp_px_cat_g1v2
        (
            id,
            cat,
            subcat,
            maintenance
        )
        SELECT 
            REPLACE(id,'_','-') AS id,
            cat,
            subcat,
            maintenance
        FROM bronz.erp_px_cat_g1v2;

        SET @end_time = GETDATE();
        PRINT 'Load Duration ERP_PX_CAT_G1V2: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
---------------------------------------------------------------------------------------------------
        -- Total Silver Layer Load Duration
        SET @P_end_time = GETDATE();
        PRINT '=================================================';
        PRINT 'Total Silver Layer Load Duration: ' + CAST(DATEDIFF(SECOND, @P_start_time, @P_end_time) AS NVARCHAR) + ' seconds';
        PRINT '=================================================';
---------------------------------------------------------------------------------------------------
    END TRY
    BEGIN CATCH
        PRINT '==========================================';
        PRINT 'ERROR OCCURRED DURING LOADING SILVER LAYER';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error State: ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '==========================================';
    END CATCH;
END;
