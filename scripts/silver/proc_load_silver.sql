create or alter procedure silver.load_silver as 
begin
insert into silver.crm_cust_info (
cst_id,cst_key,cst_firstname,cst_lastname,cst_matrial_status,cst_gndr,dwh_create_date)

 select cst_id,
cst_key,
trim(cst_firstname) as fname,
trim(cst_lastname) as lname,
case when upper(trim(cst_matrial_status))='S' then 'single'
when upper(trim(cst_matrial_status))='M' then 'Married'
else 'N/A'
end
cst_matrial_status,
case when upper(trim(cst_gndr))='F' then 'Femail'
when upper(trim(cst_gndr))='M' then 'Male'
else 'N/A'
end
cst_gndr,
cst_create_date
 from(select *,row_number() over(partition by cst_id order by cst_gndr)as flag_last  from bronze.crm_cust_info)t where flag_last=1  ;

 insert into silver.crm_prd_info(
prd_id,

prd_key,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt)
select

replace(substring(prd_key,1,5),'-','_') as prd_id,
substring(prd_key,7,len(prd_key)) as prd_key,
prd_nm,
isnull(prd_cost,0) as prd_cost,

case upper(trim(prd_line))when  'M' then 'Mountain'
when 'R' then 'Road'
when 'S' then 'Other Sales'
when 'T' then 'Touring'
else 'N/A'
end as prd_line,
cast(prd_start_dt as date)as prd_start_dt,
cast(lead(prd_start_dt) over(partition by prd_key order by prd_start_dt)-1 as date) as prd_end_dt
from bronze.crm_prd_info;

INSERT INTO silver.crm_sales_details (
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price)
select 
sls_ord_num,
sls_prd_key,
sls_cust_id,

case when sls_order_dt=0 or len(sls_order_dt)!=8 then null
else cast(cast(sls_order_dt as varchar) as date)
end as sls_order_dt,
sls_ship_dt,
case when sls_ship_dt=0 or len(sls_ship_dt)!=8 then null
else cast(cast(sls_ship_dt as varchar) as date)
end as sls_ship_dt,

sls_due_dt,
case when sls_sales is null or sls_sales<=0 or sls_sales!=sls_quantity*abs(sls_prise)
then sls_quantity*abs(sls_prise)
else sls_sales end sls_sales,
sls_quantity,
case when sls_prise is null or sls_prise <=0
then sls_sales/nullif(sls_quantity,0)
else sls_prise
end as sls_prise
from bronze.crm_sales_details;


insert into silver.erp_cust_az12(cid,bdate,gen)
select 
case when cid like 'NAS%' then substring(cid,4,len(cid))
else cid
end cid,case when bdate> getdate() then null
else bdate end as bdate,case when upper(trim(gen)) in ('F','FEMALE') then 'Female'
when upper(trim(gen)) in ('M','MALE') then 'Male'
else 'N/A' end as gen from bronze.erp_cust_az12;

insert into silver.erp_px_cat_g1v2(id,cat,subcat,maintenance)
select
id,
cat,
subcat,
maintenance
from bronze.erp_px_cat_g1v2;
end
