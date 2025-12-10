/*
=====================================================================================================================================
				DDL Scripts : Create Tables for silver Layer
====================================================================================================================================
Scripts Purpose:
	This scripts create tables for the silver schema. It drops table if it already exists and redefine the 
	DDL structure. 
=======================================================================================================================================
*/


IF OBJECT_ID('silver.employees', 'U') IS NOT NULL
DROP TABLE silver.employees;
GO

CREATE TABLE silver.employees (
		employee_id			          NVARCHAR(10),
		first_name			          NVARCHAR(50),
		last_name			            NVARCHAR(50),
		gender				            NVARCHAR(50),
		birth_date			          DATE,
		hire_date			            DATE,
		email				              NVARCHAR(70),
		phone				              NVARCHAR(20),
		city				              NVARCHAR(50),
		states				            NVARCHAR(50),
		zip					              NVARCHAR(10),
		country				            NVARCHAR(50),
		department_id		          NVARCHAR(10),
		job_title			            NVARCHAR(70),
		employment_status	        NVARCHAR(50),
		full_time			            NVARCHAR(50),
		manager_id			          NVARCHAR(10),
		department_name		        NVARCHAR(50),
        locations			        NVARCHAR(50)
);

GO

IF OBJECT_ID('silver.salaries', 'U') IS NOT NULL
DROP TABLE silver.salaries;
GO

CREATE TABLE silver.salaries(
		employee_id			    NVARCHAR(10),
		salary_gross		    INT,
		currency			      NVARCHAR(10),
		pay_frequently		  NVARCHAR(50),
		bonus				        INT,
		effective_date		  DATE

);
GO

IF OBJECT_ID('silver.departments', 'U') IS NOT NULL
DROP TABLE silver.departments;
GO

CREATE TABLE silver.departments(
		department_id		    NVARCHAR(10),
		department_name		  NVARCHAR(50),
		locations			      NVARCHAR(50)

);

GO

IF OBJECT_ID('silver.performance_reviews', 'U') IS NOT NULL
DROP TABLE silver.performance_reviews;
GO

CREATE TABLE silver.performance_reviews(
    review_id					            NVARCHAR(10),
		employee_id					          NVARCHAR(10),
		review_date					          DATE,
		reviewer_id					          NVARCHAR(10),
		rating						            INT,
  goals_met					              NVARCHAR(50),
		promotion_recommendation	    NVARCHAR(50),
		comments					            NVARCHAR(100)

);
GO
