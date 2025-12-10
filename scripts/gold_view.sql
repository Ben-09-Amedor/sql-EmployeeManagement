 /*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

            -- =============================================================================
            -- Create Dimension: gold.dim_customers
            -- =============================================================================
    
    IF OBJECT_ID('gold.dim_employees', 'V') IS NOT NULL
        DROP VIEW gold.dim_employees;
    GO

    CREATE VIEW gold.dim_employees AS
    SELECT
            ROW_NUMBER() OVER (ORDER BY employee_id) AS employee_key, -- Surrogate keys
            e.employee_id, 
            ISNULL(e.first_name, 'Unknown') AS first_name,
            ISNULL(e.last_name, 'Unknown') AS last_name,
            e.gender,
            e.birth_date,
            e.hire_date,
            ISNULL(e.email, 'Unknown')  AS email,
            e.phone,
            e.city,
            e.states,
            e.zip,
            e.country,
            ISNULL(e.department_id, 'Unknown') AS department_id,
            e.job_title,
            e.employment_status,
            e.full_time AS employment_type,
            e.manager_id,
            ISNULL(d.department_name, 'Unknown') AS department_name,
            ISNULL(d. locations, 'Unknown') AS locations
    FROM silver.employees e
    LEFT JOIN silver.departments d
     ON e.department_id = d.department_id

GO
              -- =============================================================================
                -- Create Dimension: gold.dim_performance
                -- =============================================================================
    IF OBJECT_ID('gold.dim_performance', 'V') IS NOT NULL
        DROP VIEW gold.dim_performance;
    GO

    CREATE VIEW gold.dim_performance AS
    SELECT
            ROW_NUMBER() OVER (ORDER BY e.employee_id) AS performance_key, -- Surrogate keys
            e.employee_id, 
            p.review_id,
            ISNULL(e.first_name, 'Unknown') AS first_name,
            ISNULL(e.last_name, 'Unknown') AS last_name,
            e.birth_date,
            e.hire_date,
            p.review_date,
            ISNULL(e.email, 'Unknown') AS email,
            e.phone,
            e.city,
            e.states,
            e.zip,
            e.country,
            ISNULL(e.department_id, 'Unknown') AS department_id,
            e.job_title,
            e.department_name,
            e.employment_status,
            e.full_time AS employment_type,
            e.manager_id,
            p.reviewer_id,
            p.rating,
            p.promotion_recommendation,
            p.comments
    FROM silver.employees e
    LEFT JOIN silver.performance_reviews p
        ON e.employee_id = p.employee_id
     WHERE p.reviewer_id IS NOT NULL AND p.comments IS NOT NULL -- filter out employees who did not take of the review and also employees without ratings
  
   


   
            -- =============================================================================
            -- Create Fact Table: gold.fact_sales
            -- =============================================================================
    IF OBJECT_ID('gold.fact_salries', 'V') IS NOT NULL
     DROP VIEW gold.fact_salaries;
    GO

    CREATE VIEW gold.fact_salaries AS
    SELECT 
            employee_id,
            currency,
            pay_frequently AS Payment_frequency,
            salary_gross AS gross_salary,
            ISNULL(bonus, 0) AS bonus   -- for the purpose of reporting
    FROM silver.salaries 
