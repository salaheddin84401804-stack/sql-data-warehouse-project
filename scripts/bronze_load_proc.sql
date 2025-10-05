/***********************************************************************************
Script Name: 03_bronze_load_proc.sql
Description: Stored procedure to load raw data into Bronze Layer tables
Author: Salah
Created Date: 2025
Project: SQL Data Warehouse - Medallion Architecture

Purpose:
    This procedure orchestrates the loading of raw data from CSV files into the
    Bronze Layer tables using BULK INSERT. It provides comprehensive logging,
    error handling, and execution time tracking for each table load.

Procedure: bronz.load_bronz_layer
    - Truncates all bronze tables
    - Loads data from CSV files using BULK INSERT
    - Tracks load duration for each table
    - Provides detailed console logging
    - Handles errors gracefully with TRY-CATCH

Data Sources:
    CRM System: cust_info.csv, prd_info.csv, sales_details.csv
    ERP System: CUST_AZ12.csv, LOC_A101.csv, PX_CAT_G1V2.csv

Usage:
    EXEC bronz.load_bronz_layer;

Notes:
    - Update file paths in the BULK INSERT statements to match your environment
    - Ensure SQL Server has read access to the CSV file directory
    - CSV files must have headers (FIRSTROW = 2)
    - Uses TABLOCK for better performance
    - Full refresh pattern (truncate and load)
***********************************************************************************/

CREATE OR ALTER PROCEDURE bronz.load_bronz_layer
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
        PRINT 'Loading Bronze Layer';
        PRINT '=================================================';
---------------------------------------------------------------------------------------------------
        PRINT '-------------------------------------------------';
        PRINT 'Loading CRM Tables';
        PRINT '-------------------------------------------------';

        ---------------------------
        -- 1. CRM Customer Table
        ---------------------------
        SET @start_time = GETDATE();
        PRINT '1-Truncating Table: bronz.crm_cust_info';
        TRUNCATE TABLE bronz.crm_cust_info;

        PRINT '1-Inserting Data Into: bronz.crm_cust_info';
        BULK INSERT bronz.crm_cust_info
        FROM 'C:\Users\pc\Desktop\salah\datasetsforsalah\source_crm\cust_info.csv'
        WITH
        (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT 'Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
---------------------------------------------------------------------------------------------------
        ---------------------------
        -- 2. CRM Product Table
        ---------------------------
        SET @start_time = GETDATE();
        PRINT '2-Truncating Table: bronz.crm_prd_info';
        TRUNCATE TABLE bronz.crm_prd_info;

        PRINT '2-Inserting Data Into: bronz.crm_prd_info';
        BULK INSERT bronz.crm_prd_info
        FROM 'C:\Users\pc\Desktop\salah\datasetsforsalah\source_crm\prd_info.csv'
        WITH
        (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT 'Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
---------------------------------------------------------------------------------------------------
        ---------------------------
        -- 3. CRM Sales Details Table
        ---------------------------
        SET @start_time = GETDATE();
        PRINT '3-Truncating Table: bronz.crm_sales_details';
        TRUNCATE TABLE bronz.crm_sales_details;

        PRINT '3-Inserting Data Into: bronz.crm_sales_details';
        BULK INSERT bronz.crm_sales_details
        FROM 'C:\Users\pc\Desktop\salah\datasetsforsalah\source_crm\sales_details.csv'
        WITH
        (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT 'Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
---------------------------------------------------------------------------------------------------
        PRINT '-------------------------------------------------';
        PRINT 'Loading ERP Tables';
        PRINT '-------------------------------------------------';

        ---------------------------
        -- 4. ERP Customer Table
        ---------------------------
        SET @start_time = GETDATE();
        PRINT '4-Truncating Table: bronz.erp_cust_az12';
        TRUNCATE TABLE bronz.erp_cust_az12;

        PRINT '4-Inserting Data Into: bronz.erp_cust_az12';
        BULK INSERT bronz.erp_cust_az12
        FROM 'C:\Users\pc\Desktop\salah\datasetsforsalah\source_erp\CUST_AZ12.csv'
        WITH
        (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT 'Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
---------------------------------------------------------------------------------------------------
        ---------------------------
        -- 5. ERP Location Table
        ---------------------------
        SET @start_time = GETDATE();
        PRINT '5-Truncating Table: bronz.erp_loc_a101';
        TRUNCATE TABLE bronz.erp_loc_a101;

        PRINT '5-Inserting Data Into: bronz.erp_loc_a101';
        BULK INSERT bronz.erp_loc_a101
        FROM 'C:\Users\pc\Desktop\salah\datasetsforsalah\source_erp\LOC_A101.csv'
        WITH
        (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT 'Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
---------------------------------------------------------------------------------------------------
        ---------------------------
        -- 6. ERP Price Category Table
        ---------------------------
        SET @start_time = GETDATE();
        PRINT '6-Truncating Table: bronz.erp_px_cat_g1v2';
        TRUNCATE TABLE bronz.erp_px_cat_g1v2;

        PRINT '6-Inserting Data Into: bronz.erp_px_cat_g1v2';
        BULK INSERT bronz.erp_px_cat_g1v2
        FROM 'C:\Users\pc\Desktop\salah\datasetsforsalah\source_erp\PX_CAT_G1V2.csv'
        WITH
        (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT 'Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
---------------------------------------------------------------------------------------------------
        -- Total Bronze Layer Load Duration
        SET @P_end_time = GETDATE();
        PRINT '=================================================';
        PRINT 'Total Bronze Layer Load Duration: ' + CAST(DATEDIFF(SECOND, @P_start_time, @P_end_time) AS NVARCHAR) + ' seconds';
        PRINT '=================================================';

---------------------------------------------------------------------------------------------------
    END TRY
    BEGIN CATCH
        PRINT '==========================================';
        PRINT 'ERROR OCCURRED DURING LOADING BRONZE LAYER';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error State: ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '==========================================';
    END CATCH

END;
