/* 
========================================================================================================================
									Create database and schemas
========================================================================================================================
Script Purpose:
		This scripts create a new database name "EmployeesManagement" after checking if the database already exists.
		If the database already, it drops and recreat. In addition, it creates three schemas in the database; silver, bronze and gold.

Warning:
	Running this scripts will permanently delete the database "EmployeeManagement" if it already exists.
	All data in the database will be lost. proceeds with caution and ensure all data are properly backed up
	before running this scripts


*/


USE master;
GO

-- Drop and create database if it already
IF EXISTS ( SELECT 1 FROM	sys.databases WHERE name = 'EmployeesManagement')
	BEGIN 
		ALTER DATABASE EmployeesManagement SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
		DROP DATABASE EmployeesManagement;
	END;
	GO


	-- Create Database
CREATE DATABASE EmployeesManagement;
GO


USE EmployeesManagement;
GO


	-- Create Schemas
CREATE SCHEMA gold;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA bronze;
GO

