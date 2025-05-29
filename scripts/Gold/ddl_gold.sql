/*
===
DDL Script: Create Gold Views
Script Purpose:
This script creates views for the Gold layer in the data warehouse.
The Gold layer represents the final dimension and fact tables (Star Schema)
Each view performs transformations and combines data from the Silver layer to produce a clean, enriched, and business-ready dataset.
Usage:
-
These views can be queried directly for analytics and reporting.
===========
*/
--
  
-- Create Dimension: gold.dim_customers
IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL 
  DROP VIEW gold dim customers;
Go

Create view  gold.dim_customers as
select 
ROW_NUMBER() over (order by cst_id) as customer_key,
cr.cst_id as customer_id,
cr.cst_key as customer_number,
cr.cst_firstname as first_name,
cr.cst_lastname as last_name,
el.CNTRY as country,
case when cr.cst_gndr != 'N/A' then cr.cst_gndr -- CRM is Master table for Gender
      else coalesce(er.gen, 'N/A')
end as Gender,
cr.cst_marital_status as marital_status,
er.bdate birth_date,
cr.cst_create_date as create_date
from silver.crm_cust_info cr
left join silver.erp_cust_az12 er
on cr.cst_key = er.CID
left join silver.erp_loc_a101 el
on cr.cst_key = el.CID;


IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL 
  DROP VIEW gold dim customers;
Go


IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL 
  DROP VIEW gold dim customers;
Go



