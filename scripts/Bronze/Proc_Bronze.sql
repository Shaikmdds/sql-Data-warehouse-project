
create or alter procedure bronze.load_bronze as
begin
declare @start_time datetime, @end_time datetime, @start_batch_time datetime, @end_batch_time datetime
	begin try

		set @start_batch_time = GETDATE();
		print'===============================';
		print'Loading Data into Bronze Layer';
		print'===============================';

	
		print'===============================';
		print'Loading CRM Tables';
		print'===============================';
		set @start_time= GETDATE();
		print'-----------------------------';
		print'Truncating Table: bronze.crm_cust_info ';
		print'-----------------------------';

		truncate table bronze.crm_cust_info;

		print'-----------------------------';
		print'Bulk Inserting into Table: bronze.crm_cust_info ';
		print'-----------------------------';

		bulk insert bronze.crm_cust_info
		from 'C:\Users\shaik\OneDrive\Documents\All_Sql_files\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with (
		firstrow = 2,
		fieldterminator = ',',
		tablock
		);
		set @end_time = GETDATE();

		print'loading time: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' Seconds';

		select * from bronze.crm_cust_info;
	
		print'-----------------------------';
		print'Truncating Table: bronze.crm_prd_info ';
		print'-----------------------------';

		set @start_time = GETDATE();

		truncate table bronze.crm_prd_info;

	
		print'-----------------------------';
		print'Bulk Inserting into Table: bronze.crm_prd_info ';
		print'-----------------------------';

		bulk insert bronze.crm_prd_info
		from 'C:\Users\shaik\OneDrive\Documents\All_Sql_files\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with (
		firstrow = 2,
		fieldterminator = ',',
		tablock
		);

		set @end_time = GETDATE();

		print'-------------------------';
		print'loading time: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' Seconds';
		print'-------------------------';

		select * from bronze.crm_prd_info;

		print'-----------------------------';
		print'Truncating Table: bronze.crm_sales_details ';
		print'-----------------------------';

		set @start_time = GETDATE();
		truncate table bronze.crm_sales_details;

	
		print'-----------------------------';
		print'Bulk Inserting into Table: bronze.crm_sales_details ';
		print'-----------------------------';

		bulk insert bronze.crm_sales_details
		from 'C:\Users\shaik\OneDrive\Documents\All_Sql_files\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with (
		firstrow = 2,
		fieldterminator = ',',
		tablock
		);

		set @end_time = GETDATE();

		
		print'-------------------------';
		print'loading time: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' Seconds';
		print'-------------------------';

		select * from bronze.crm_sales_details;

		
		print'===============================';
		print'Loading ERP Tables';
		print'===============================';

		print'-----------------------------';
		print'Truncating Table: bronze.erp_cust_az12 ';
		print'-----------------------------';

		set @start_time = GETDATE();

		truncate table bronze.erp_cust_az12;

	
		print'-----------------------------';
		print'Bulk Inserting into Table: bronze.erp_cust_az12 ';
		print'-----------------------------';

		bulk insert bronze.erp_cust_az12
		from 'C:\Users\shaik\OneDrive\Documents\All_Sql_files\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		with (
		firstrow = 2,
		fieldterminator = ',',
		tablock
		);

		set @end_time = GETDATE();

		
		print'-------------------------';
		print'loading time: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' Seconds';
		print'-------------------------';

		select * from bronze.erp_cust_az12;



		print'-----------------------------';
		print'Truncating Table: bronze.erp_loc_a101 ';
		print'-----------------------------';
		
		set @start_time = GETDATE();

		truncate table bronze.erp_loc_a101;

	
		print'-----------------------------';
		print'Bulk Inserting into Table: bronze.erp_loc_a101 ';
		print'-----------------------------';

		bulk insert bronze.erp_loc_a101
		from 'C:\Users\shaik\OneDrive\Documents\All_Sql_files\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		with (
		firstrow = 2,
		fieldterminator = ',',
		tablock
		);
		set @end_time = GETDATE();

		
		print'-------------------------';
		print'loading time: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' Seconds';
		print'-------------------------';

		select * from bronze.erp_loc_a101;

		print'-----------------------------';
		print'Truncating Table: bronze.erp_px_cat_g1v2 ';
		print'-----------------------------';
		
		set @start_time = GETDATE();


		truncate table bronze.erp_px_cat_g1v2;

	
		print'-----------------------------';
		print'Bulk Inserting into Table: bronze.erp_px_cat_g1v2 ';
		print'-----------------------------';

		bulk insert bronze.erp_px_cat_g1v2
		from 'C:\Users\shaik\OneDrive\Documents\All_Sql_files\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		with (
		firstrow = 2,
		fieldterminator = ',',
		tablock
		);
		set @end_time = GETDATE();

		
		print'-------------------------';
		print'loading time: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' Seconds';
		print'-------------------------';

		set @end_batch_time = GETDATE();
				
		print'-------------------------';
		print' Loading Bronze layer Completed';
		print' Loading time: ' + cast(datediff(second, @start_batch_time, @end_batch_time) as nvarchar) + ' Seconds';
		print'-------------------------';

	end try
	begin catch

	print'---------------------------';
	print'error' + error_message();
	print'error number' + cast(error_number() as nvarchar);
	print'error state' + cast(error_state() as nvarchar);
	end catch

end
