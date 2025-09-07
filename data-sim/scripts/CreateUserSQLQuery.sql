-- Check if login exists before creating
IF NOT EXISTS (SELECT * FROM sys.server_principals WHERE name = 'mercy')
BEGIN
    CREATE LOGIN mercy WITH PASSWORD = 'mercy';
END
GO

-- Switch to your database
USE azure_clinical_data_warehouse;
GO

-- Check if database user exists before creating
IF NOT EXISTS (SELECT * FROM sys.database_principals WHERE name = 'mercy')
BEGIN
    CREATE USER mercy FOR LOGIN mercy;
    ALTER ROLE db_datareader ADD MEMBER mercy;  -- Grant read access
END
GO

-- Test login
--Connect with SSMS using login 'mercy', query 
SELECT TOP 5 * FROM registry;
