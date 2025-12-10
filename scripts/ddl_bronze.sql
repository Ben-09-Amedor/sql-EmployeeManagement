/*
=====================================================================================================================================
				DDL Scripts : Create Tables for Bronze Layer
====================================================================================================================================
Scripts Purpose:
	This scripts create tables for the bronze schema. It drops table if it already exists and redefine the 
	DDL structure. 
*/

IF OBJECT_ID('bronze.employees', 'U') IS NOT NULL
DROP TABLE bronze.employees;
GO

CREATE TABLE bronze.employees (
		employee_id			      NVARCHAR(10),
		first_name			      NVARCHAR(50),
		last_name			        NVARCHAR(50),
		gender				        NVARCHAR(50),
		birth_date			      DATE,
		hire_date			        DATE,
		email				          NVARCHAR(70),
		phone				          NVARCHAR(20),
		city				          NVARCHAR(50),
		states				        NVARCHAR(50),
		zip					          NVARCHAR(10),
		country				        NVARCHAR(50),
		department_id		      NVARCHAR(10),
		job_title			        NVARCHAR(70),
		employment_status    	NVARCHAR(50),
		full_time			        NVARCHAR(50),
		manager_id			      NVARCHAR(10)

);

GO

IF OBJECT_ID('bronze.salaries', 'U') IS NOT NULL
DROP TABLE bronze.salaries;
GO

CREATE TABLE bronze.salaries(
		employee_id			    NVARCHAR(10),
		salary_gross		    INT,
		currency			      NVARCHAR(10),
		pay_frequently		  NVARCHAR(50),
		bonus				        INT,
		effective_date		  DATE

);
GO

IF OBJECT_ID('bronze.departments', 'U') IS NOT NULL
DROP TABLE bronze.departments;
GO

CREATE TABLE bronze.departments(
		department_id		      NVARCHAR(10),
		department_name		    NVARCHAR(50),
		locations			        NVARCHAR(50)

);

GO

IF OBJECT_ID('bronze.performance_reviews', 'U') IS NOT NULL
DROP TABLE bronze.performance_reviews;
GO

CREATE TABLE bronze.performance_reviews(
		review_id					         NVARCHAR(10),
		employee_id					       NVARCHAR(10),
		review_date					       DATE,
    reviewer_id					       NVARCHAR(10),
		rating						         NVARCHAR(10),
		goals_met					         NVARCHAR(50),
		promotion_recommendation	NVARCHAR(50),
		comments					        NVARCHAR(100)

);
GO




