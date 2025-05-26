/*
===================================================================================================
    Procedure: bronze.load_bronze
    Purpose  : Load raw/staging ("bronze layer") data from flat files (CSV format)
               into corresponding SQL Server tables using BULK INSERT. This procedure simulates 
               an initial raw data ingestion step in a typical Data Warehouse ETL pipeline.

    Functionality:
    - Truncates existing data in bronze tables to avoid duplicate loads.
    - Loads CRM and ERP data from local CSV files using BULK INSERT.
    - Tracks time taken to load each table and overall batch duration.
    - Error handling included via TRY...CATCH block.

    ⚠️ NOTE:
    - File paths are hardcoded for local execution. Consider parameterization or dynamic pathing 
      for production usage.
    - Files must exist and be accessible at the specified path.
    - Ensure proper permissions for BULK INSERT operation.
    Usage Example:
        EXEC bronze.load_bronze;
===================================================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
BEGIN
	DECLARE @batch_start_time DATETIME, @batch_end_time DATETIME;
	DECLARE @start_time DATETIME, @end_time DATETIME;

	BEGIN TRY
		SET @batch_start_time = GETDATE();

		PRINT '=========================================================================================';
		PRINT '                               STARTING BRONZE LAYER LOAD                                ';
		PRINT '=========================================================================================';

		-- ======================= CRM TABLES LOADING ==========================
		PRINT '-----------------------------------------------------------------------------------------';
		PRINT 'LOADING CRM TABLES';
		PRINT '-----------------------------------------------------------------------------------------';

		-- Load crm_cust_info
		PRINT '>> Truncating Table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;

		PRINT '>> Inserting Data Into: bronze.crm_cust_info';
		SET @start_time = GETDATE();
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\LENOVO\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '-------------------------------------------------';

		-- Load crm_prd_info
		PRINT '>> Truncating Table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT '>> Inserting Data Into: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\LENOVO\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		-- Load crm_sales_details
		PRINT '>> Truncating Table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT '>> Inserting Data Into: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\LENOVO\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		-- ======================= ERP TABLES LOADING ==========================
		PRINT '-----------------------------------------------------------------------------------------';
		PRINT 'LOADING ERP TABLES';
		PRINT '-----------------------------------------------------------------------------------------';

		-- Load erp_cust_az12
		PRINT '>> Truncating Table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;

		PRINT '>> Inserting Data Into: bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\LENOVO\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		-- Load erp_loc_a101
		PRINT '>> Truncating Table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;

		PRINT '>> Inserting Data Into: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\LENOVO\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		-- Load erp_px_cat_g1v2
		PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		PRINT '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\LENOVO\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		-- ======================= BATCH COMPLETION ==========================
		SET @batch_end_time = GETDATE();
		PRINT '>> TOTAL BATCH LOAD DURATION: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=========================================================================================';
		PRINT '                                BRONZE LAYER LOAD COMPLETED                              ';
		PRINT '=========================================================================================';

	END TRY

	BEGIN CATCH
		PRINT '‼ ERROR OCCURRED DURING BRONZE LAYER LOADING';
		PRINT ERROR_MESSAGE();  -- Optional: print detailed error message
	END CATCH
END;
