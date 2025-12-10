	/*
===============================================================================
Magnitude Analysis
===============================================================================
Purpose:
    - To quantify data and group results by specific dimensions.
    - For understanding data distribution across categories.

SQL Functions Used:
    - Aggregate Functions: SUM(), COUNT(), AVG()
    - GROUP BY, ORDER BY, HAVING
===============================================================================
*/

-- total number of employees by gender
SELECT gender, count(*) AS total_employee
FROM gold.dim_employees
GROUP BY gender;



-- total number of employees by location
SELECT locations, COUNT(*) AS total_number
FROM gold.dim_employees
GROUP BY locations
ORDER BY locations;

-- find employees by nature of employement
SELECT employment_type, COUNT (*) AS total_employees
FROM gold.dim_employees
GROUP BY employment_type;
