/*******************************************************************************************
-- Script Name   : init_database.sql
-- Purpose       : 
--   This script creates a new SQL Server database named 'DataWarehouse' along with 
--   three schemas: bronze, silver, and gold. These schemas are typically used to organize 
--   data at different stages in a data warehousing pipeline:
--     - bronze: raw/landing data
--     - silver: cleaned/transformed data
--     - gold: curated/analytics-ready data

-- Warning       :
--   ⚠️ This script drops the existing 'DataWarehouse' database if it exists.
--   ⚠️ All data in the current 'DataWarehouse' will be permanently lost.
--   ⚠️ Ensure that no important data is stored or that a backup exists before running this script.

-- Author        : [Your Name]
-- Date Created  : [Date]
*******************************************************************************************/

-- Use the 'master' database to perform administrative tasks like creating/dropping a database
USE master;
GO

-- Drop the existing 'DataWarehouse' database if it exists
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    -- Force disconnect all users and rollback active transactions
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;

    -- Drop the existing database
    DROP DATABASE DataWarehouse;
END;
GO

-- Create a new empty 'DataWarehouse' database
CREATE DATABASE DataWarehouse;
GO

-- Switch context to the newly created 'DataWarehouse' database
USE DataWarehouse;
GO

-- Create schema for raw or landing data
CREATE SCHEMA bronze;
GO

-- Create schema for cleaned/transformed data
CREATE SCHEMA silver;
GO

-- Create schema for curated/analytics-ready data
CREATE SCHEMA gold;
GO
