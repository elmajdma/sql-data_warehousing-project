EXEC bronze.load_bronze;


CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
DECLARE @Start_time DATETIME, @End_time DATETIME, @batch_start DATETIME, @batch_end DATETIME;
	BEGIN TRY
	SET @batch_start=GETDATE();
	PRINT'=====================================================';
	PRINT 'LOADING BRONZE LAYER!';
	PRINT'=====================================================';
	PRINT'-----------------------------------------------------';
	PRINT'LOADING CRM DATA ';
	PRINT'-----------------------------------------------------';
		SET @Start_time=GETDATE();
		PRINT'>>Truncate Table bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;

		PRINT'>>Load Table bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'E:\sql-data-warehouse-project\source_crm\cust_info.csv'
		WITH (
				FIRSTROW=2,
				FIELDTERMINATOR= ',',
				TABLOCK
				);
		SET @End_time=GETDATE();
		PRINT'Loading Duration: '+ CAST(DATEDIFF(second, @Start_time, @End_time) AS VARCHAR)+ 'Second';


		PRINT'>>Truncate Table bronze.crm_prd_info'
		TRUNCATE TABLE bronze.crm_prd_info;
		BULK INSERT bronze.crm_prd_info
		FROM 'E:\sql-data-warehouse-project\source_crm\prd_info.csv'
		WITH (
				FIRSTROW=2,
				FIELDTERMINATOR= ',',
				TABLOCK
				);

		PRINT'>>Truncate Table bronze.crm_sales_details'
		TRUNCATE TABLE bronze.crm_sales_details;
		BULK INSERT bronze.crm_sales_details
		FROM 'E:\sql-data-warehouse-project\source_crm\sales_details.csv'
		WITH (
				FIRSTROW=2,
				FIELDTERMINATOR= ',',
				TABLOCK
				);
	PRINT'-----------------------------------------------------'
	PRINT'LOADING ERP DATA '
	PRINT'-----------------------------------------------------'
		
		PRINT'>>Truncate Table bronze.erp_cust_az12'
		TRUNCATE TABLE bronze.erp_cust_az12;
		BULK INSERT bronze.erp_cust_az12
		FROM 'E:\sql-data-warehouse-project\source_erp\CUST_AZ12.csv'
		WITH (
				FIRSTROW=2,
				FIELDTERMINATOR= ',',
				TABLOCK
				);
		
		PRINT'>>Truncate Table bronze.erp_loc_a101'
		TRUNCATE TABLE bronze.erp_loc_a101;
		BULK INSERT bronze.erp_loc_a101
		FROM 'E:\sql-data-warehouse-project\source_erp\LOC_A101.csv'
		WITH (
				FIRSTROW=2,
				FIELDTERMINATOR= ',',
				TABLOCK
				);
		
		PRINT'>>Truncate Table bronze.erp_px_cat_g1v2'
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'E:\sql-data-warehouse-project\source_erp\PX_CAT_G1V2.csv'
		WITH (
				FIRSTROW=2,
				FIELDTERMINATOR= ',',
				TABLOCK
				);
	SET @batch_end=GETDATE();
	PRINT'Total Laoding Duration: '+CAST(DATEDIFF(second, @batch_start, @batch_end) AS VARCHAR) +' Seconds';
	END TRY
	BEGIN CATCH
	PRINT'====================================================='
	PRINT'Error Message: '+ERROR_MESSAGE();
	PRINT 'Error Number: '+CAST(ERROR_NUMBER() AS VARCHAR);
	PRINT 'Error state: '+CAST(ERROR_STATE() AS VARCHAR)
	PRINT'====================================================='
	END CATCH
END