/*
Quality Checks
===========
Script Purpose:
This script performs various quality checks for data consistency, accuracy,
and standardization across the 'silver' schema. It includes checks for:
-
Null or duplicate primary keys.
-
Unwanted
aces in string fields.
Ices

   
- Data stanedization and consistency.
-
Invalid date ranges and orders.
- Data consistency between related fields.
Usage Notes:
-
Run these checks after data loading Silver Layer.
Investigate and resolve any discrepancies found during the checks.
=========
======
============
*/


select * from bronze.crm_cust_info;

-- Check For Duplicates or Null Values in the Primary key
-- Expectations : No Result
select cst_id, count(*)
from silver.crm_cust_info
group by cst_id
having count(*) > 1 or cst_id is null;

-- Trimming the string values
select cst_firstname
from silver.crm_cust_info
where cst_firstname != TRIM(cst_firstname);

select cst_lastname
from silver.crm_cust_info
where cst_lastname != TRIM(cst_lastname);

--checking the no of values in a low cardinality coloumn
select distinct cst_marital_status from silver.crm_cust_info; 


select distinct cst_gndr from silver.crm_cust_info; 

select cst_id, count(*)
from silver.crm_cust_info
group by cst_id
having count(*) > 1 or cst_id is null;



select * from bronze.crm_prd_info;

-- Check For Duplicates or Null Values in the Primary key
-- Expectations : No Result
select prd_id, count(*)
from silver.crm_prd_info
group by prd_id
having count(*) > 1 or prd_id is null;

-- Trimming the string values
select prd_nm
from silver.crm_prd_info
where prd_nm != TRIM(prd_nm);

select cst_lastname
from silver.crm_cust_info
where cst_lastname != TRIM(cst_lastname);

--checking the no of values in a low cardinality coloumn
select  prd_cost from silver.crm_prd_info where prd_cost is null; 


select distinct prd_line from silver.crm_prd_info; 

select *
from silver.crm_prd_info
where prd_start_dt > prd_end_dt;



select cst_id, count(*)
from silver.crm_cust_info
group by cst_id
having count(*) > 1 or cst_id is null;



select * from bronze.crm_prd_info;

-- Check For Duplicates or Null Values in the Primary key
-- Expectations : No Result
select sls_ord_num, count(*)
from bronze.crm_sales_details
group by sls_ord_num
having count(*) > 1 or sls_ord_num is null;

-- Trimming the string values
select prd_nm
from silver.crm_prd_info
where prd_nm != TRIM(prd_nm);

select cst_lastname
from silver.crm_cust_info
where cst_lastname != TRIM(cst_lastname);

--checking the no of values in a low cardinality coloumn
select  prd_cost from silver.crm_prd_info where prd_cost is null; 


select distinct prd_line from silver.crm_prd_info; 

select *
from silver.crm_prd_info
where prd_start_dt > prd_end_dt;


select * from bronze.crm_sales_details

SELECT * FROM bronze.erp_cust_az12;


select  case when  BDATE > GETDATE() then null
   else bdate end as bdate
from bronze.erp_cust_az12;

select bdate from silver.erp_cust_az12
where bdate > GETDATE();

select cid from (
select case when cid like 'NASA%' then trim(substring(cid, 4,len(cid)))
       else cid 
end as cid
from bronze.erp_cust_az12) ne
where cid not in (select cst_key from silver.crm_cust_info)


select distinct gen from(
select case when gen  in ('M', 'Male') then 'Male'
when gen  in ('F', 'Female') then 'female'
 else 'N/A'
 end as gen
 from  bronze.erp_cust_az12)l
  
select * from bronze.erp_loc_a101;



select distinct cntry from silver.erp_loc_a101;


select case when cntry in ('USA','US','United States') then 'USA'
when cntry in ('DE', 'Germany') then 'Germany'
when CNTRY in ('') or cntry is null then 'N/A'
else cntry end as cntry

from bronze.erp_loc_a101;


select cst_key from silver.crm_cust_info;


select REPLACE(cid,'-','') as CID

from bronze.erp_loc_a101;


select CID from silver.erp_loc_a101;


select id from bronze.erp_px_cat_g1v2
where id not in (select cat_id from silver.crm_prd_info);

select cat from bronze.erp_px_cat_g1v2
where cat not in (select trim(cat) from bronze.erp_px_cat_g1v2);

select distinct subcat from bronze.erp_px_cat_g1v2;

select * from bronze.erp_px_cat_g1v2;
select * from silver.crm_prd_info;
