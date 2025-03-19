CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT 'Loading data into bronze tables...';
        PRINT '----------------------------------'
        PRINT 'Loading data into bronze CRM Tables...';
        PRINT '----------------------------------'
        
        -- Load crm_cust_info
        SET @start_time = GETDATE();
        PRINT '>>Truncating table bronze.crm_cust_info...';
        TRUNCATE TABLE bronze.crm_cust_info;
        
        PRINT '>>Inserting data into table bronze.crm_cust_info...';
        BULK INSERT bronze.crm_cust_info
        FROM 'C:\Users\Federico\Documents\Coder House\Data Science\Proyectos Personales\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        WITH
        (
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            FIRSTROW = 2,
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT 'Time taken to load bronze.crm_cust_info: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        PRINT '>>>>Truncating table bronze.crm_prd_info...';
        TRUNCATE TABLE bronze.crm_prd_info;
        
        PRINT '>>Inserting data into table bronze.crm_prd_info...';
        BULK INSERT bronze.crm_prd_info
        FROM 'C:\Users\Federico\Documents\Coder House\Data Science\Proyectos Personales\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        WITH
        (
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            FIRSTROW = 2,
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT 'Time taken to load bronze.crm_prd_info: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        -- Load crm_sales_details
        SET @start_time = GETDATE();
        PRINT '>>Truncating table bronze.crm_sales_details...';
        TRUNCATE TABLE bronze.crm_sales_details;
        
        PRINT '>>Inserting data into table bronze.crm_sales_details...';
        BULK INSERT bronze.crm_sales_details
        FROM 'C:\Users\Federico\Documents\Coder House\Data Science\Proyectos Personales\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        WITH
        (
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            FIRSTROW = 2,
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT 'Time taken to load bronze.crm_sales_details: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        PRINT '----------------------------------'
        PRINT 'Loading data into bronze ERP Tables...';
        PRINT '----------------------------------'
        
        -- Load erp_cust_az12
        SET @start_time = GETDATE();
        PRINT '>>Truncating table bronze.erp_cust_az12...';
        TRUNCATE Table bronze.erp_cust_az12;
        
        PRINT '>>Inserting data into table bronze.erp_cust_az12...';
        BULK INSERT bronze.erp_cust_az12
        FROM 'C:\Users\Federico\Documents\Coder House\Data Science\Proyectos Personales\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
        WITH
        (
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            FIRSTROW = 2,
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT 'Time taken to load bronze.erp_cust_az12: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        -- Load erp_loc_a101
        SET @start_time = GETDATE();
        PRINT '>>Truncating table bronze.erp_loc_a101...';
        TRUNCATE TABLE bronze.erp_loc_a101;
        
        PRINT '>>Inserting data into table bronze.erp_loc_a101...';
        BULK INSERT bronze.erp_loc_a101
        FROM 'C:\Users\Federico\Documents\Coder House\Data Science\Proyectos Personales\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
        WITH
        (
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            FIRSTROW = 2,
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT 'Time taken to load bronze.erp_loc_a101: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        -- Load erp_px_cat_g1v2
        SET @start_time = GETDATE();
        PRINT '>>Truncating table bronze.erp_px_cat_g1v2...';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;
        
        PRINT '>>Inserting data into table bronze.erp_px_cat_g1v2...';
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'C:\Users\Federico\Documents\Coder House\Data Science\Proyectos Personales\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
        WITH
        (
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            FIRSTROW = 2,
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT 'Time taken to load bronze.erp_px_cat_g1v2: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        SET @batch_end_time = GETDATE();
        PRINT '----------------------------------'
        PRINT 'Time taken to load all bronze tables: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
        PRINT '----------------------------------'
    END TRY
    BEGIN CATCH
        PRINT '----------------------------------'
        PRINT 'An error occurred during loading bronze layer: ' + ERROR_MESSAGE();
        PRINT CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT CAST(ERROR_SEVERITY() AS NVARCHAR);
        PRINT CAST(ERROR_STATE() AS NVARCHAR);
    END CATCH
END
