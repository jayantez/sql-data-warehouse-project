/*
==========================================================================================
    Script Purpose:
    This script is used to create the base layer ("bronze") staging tables 
    for loading raw data from multiple source systems (CRM, ERP, etc.) 
    into a Data Warehouse. 

    It ensures clean initialization by dropping existing tables if they already exist, 
    and then recreating them with defined schemas for further ETL processing.

    WARNING:
    ⚠️ This script will permanently drop existing tables in the 'bronze' schema. 
    Any existing data will be lost. Ensure backup or export is taken if required 
    before executing.
==========================================================================================
*/

-- Drop and recreate CRM Customer Info table
IF OBJECT_ID('bronze.crm_cust_info' , 'U') IS NOT NULL
	DROP TABLE bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info (
    cst_id INT,                            -- Internal customer ID
    cst_key NVARCHAR(50),                 -- Business/customer key
    cst_firstname NVARCHAR(50),           -- Customer first name
    cst_lastname NVARCHAR(50),            -- Customer last name
    cst_material_status NVARCHAR(50),     -- Marital or material status
    cst_gndr NVARCHAR(50),                -- Gender
    cst_create_date DATE                  -- Customer creation date
);

-- Drop and recreate CRM Product Info table
IF OBJECT_ID('bronze.crm_prd_info' , 'U') IS NOT NULL
	DROP TABLE bronze.crm_prd_info;
CREATE TABLE bronze.crm_prd_info (
    prd_id INT,                           -- Unique product ID
    prd_key NVARCHAR(50),                -- Business or alternate key
    prd_nm NVARCHAR(50),                 -- Product name
    prd_cost INT,                        -- Product cost
    prd_line NVARCHAR(50),               -- Product category or line
    prd_start_dt DATETIME,              -- Product availability start date
    prd_end_dt DATETIME                 -- Product availability end date
);

-- Drop and recreate CRM Sales Details table
IF OBJECT_ID('bronze.crm_sales_details' , 'U') IS NOT NULL
	DROP TABLE bronze.crm_sales_details;
CREATE TABLE bronze.crm_sales_details (
    sls_ord_num NVARCHAR(50),           -- Unique sales order number
    sls_prd_key NVARCHAR(50),           -- Product key
    sls_cust_id INT,                    -- Customer ID
    sls_order_dt INT,                   -- Order date (format: YYYYMMDD)
    sls_ship_dt INT,                    -- Shipment date (format: YYYYMMDD)
    sls_due_dt INT,                     -- Due date (format: YYYYMMDD)
    sls_sales INT,                      -- Total sales amount
    sls_quantity INT,                   -- Quantity ordered
    sls_price INT                       -- Price per unit
);

-- Drop and recreate ERP Customer Basic Info table
IF OBJECT_ID('bronze.erp_cust_az12' , 'U') IS NOT NULL
	DROP TABLE bronze.erp_cust_az12;
CREATE TABLE bronze.erp_cust_az12 (
    cid NVARCHAR(50),                   -- Unique customer identifier
    bdate DATE,                         -- Birthdate
    gen VARCHAR(50)                     -- Gender
);

-- Drop and recreate ERP Customer Location Info table
IF OBJECT_ID('bronze.erp_loc_a101' , 'U') IS NOT NULL
	DROP TABLE bronze.erp_loc_a101;
CREATE TABLE bronze.erp_loc_a101 (
    cid NVARCHAR(50),                   -- Customer ID
    cntry VARCHAR(50)                   -- Country of residence
);

-- Drop and recreate ERP Product Category Info table
IF OBJECT_ID('bronze.erp_px_cat_g1v2' , 'U') IS NOT NULL
	DROP TABLE bronze.erp_px_cat_g1v2;
CREATE TABLE bronze.erp_px_cat_g1v2 (
    id NVARCHAR(50),                    -- Unique ID (e.g., AC_BR)
    cat NVARCHAR(50),                   -- Product category (e.g., Accessories)
    subcat NVARCHAR(50),                -- Product sub-category (e.g., Bike Racks)
    maintenance NVARCHAR(50)            -- Maintenance required (Yes/No)
);
