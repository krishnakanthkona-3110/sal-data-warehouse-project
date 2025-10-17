select *
from
(
select *,
row_number() over(partition by cst_id order by cst_create_date desc) flag
from bronze.crm_cust_info) t
where flag =1

-- check for unwanted spaces
-- Expectation: No Spaces
select cst_firstname
from bronze.crm_cust_info
where cst_firstname != trim(cst_firstname)

/*
If the original value is not equal to the same value after trimming,
it means there are spaces! -- it is not good
*/
select cst_lastname
from bronze.crm_cust_info
where cst_lastname != trim(cst_lastname)

select cst_gndr
from bronze.crm_cust_info
where cst_gndr != trim(cst_gndr)

-- Gender is better, as there are no spaces

-- remove all the spaces
SELECT cst_id
      ,cst_key
      ,trim(cst_firstname)
      ,trim(cst_lastname)
      ,cst_marital_status
      ,cst_gndr
      ,cst_create_date
  FROM DataWarehouse.bronze.crm_cust_info

  -- Data Standardization and Consistency
  select distinct cst_gndr
  from bronze.crm_cust_info

  select distinct cst_marital_status
  from bronze.crm_cust_info


  -- In our data warehouse, we aim to store clear and meaningful value
  -- rather than using abbreviated terms.
  -- Instead of F - Female, and Male for M.
  -- In our data warehouse, we use the defaultvalue 'n/a' for missing values
  -- apply upper() just in case mixed-case values appear later in your column

insert into silver.crm_cust_info

  (cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status,cst_gndr, cst_create_date)

  SELECT cst_id
      ,cst_key
      ,trim(cst_firstname) as cst_firstname
      ,trim(cst_lastname) as cst_lastname,
  case when upper(trim(cst_marital_status))='S' then 'Single'
            when upper(trim(cst_marital_status))='M' then 'Married'
            else 'n/a'
            end cst_marital_status, 
      case when upper(trim(cst_gndr))='F' then 'Female'
            when upper(trim(cst_gndr))='M' then 'Male'
            else 'n/a'
            end cst_gndr,
            cst_create_date
  FROM 
  (
select *,
row_number() over(partition by cst_id order by cst_create_date desc) as flag_last
from bronze.crm_cust_info 
where cst_id is not null) t
where flag_last =1;



  select * from silver.crm_cust_info
