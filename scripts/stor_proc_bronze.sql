
/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/


CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '================================================';
		PRINT 'Loading Bronze Layer';
		PRINT '================================================';

		
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.employees';
		TRUNCATE TABLE bronze.employees;
		BULK INSERT bronze.employees
		FROM 'C:\Users\Pc\Desktop\DATA FOR ANALYSIS\MSS SQL FOLDER\PROJECT_4\dataset\employees.csv'
		WITH(
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';


		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.salaries';
		TRUNCATE TABLE bronze.salaries;
		BULK INSERT bronze.salaries
		FROM 'C:\Users\Pc\Desktop\DATA FOR ANALYSIS\MSS SQL FOLDER\PROJECT_4\dataset\salaries.csv'
		WITH(
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			);
		SET @end_time = GETDATE();
		PRINT '>> Load duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>>-----------------------------------------------------';


		SET @start_time = GETDATE()
		PRINT '>> Truncating Table: bronze.departments';
		TRUNCATE TABLE bronze.departments;
		BULK INSERT bronze.departments
		FROM 'C:\Users\Pc\Desktop\DATA FOR ANALYSIS\MSS SQL FOLDER\PROJECT_4\dataset\departments.csv'
		WITH(
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			);
		SET @end_time = GETDATE();
		PRINT '>> Load duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + 'seconds';
		PRINT '>>--------------------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT 'Truncting Table: bronze.performance_reviews';
		TRUNCATE TABLE bronze.performance_reviews;
		BULK INSERT bronze.performance_reviews
		FROM 'C:\Users\Pc\Desktop\DATA FOR ANALYSIS\MSS SQL FOLDER\PROJECT_4\dataset\performance_reviews.csv'
		WITH(
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			);
		SET @end_time = GETDATE();
		PRINT 'Load duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>>------------------------------------------------------------------';

		SET @batch_end_time = GETDATE();
		PRINT '=========================================='
		PRINT 'Loading Bronze Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=========================================='
	END TRY

	BEGIN CATCH
		PRINT '==========================================';
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '==========================================';
	END CATCH
END
