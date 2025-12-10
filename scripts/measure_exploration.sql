*
===============================================================================
Measures Exploration (Key Metrics)
===============================================================================
Purpose:
    - To calculate aggregated metrics (e.g., totals, averages) for quick insights.
    - To identify overall trends or spot anomalies.

SQL Functions Used:
    - COUNT(), SUM(), AVG()
===============================================================================
*/

-- count total number of employees
SELECT count(employee_id) AS Total_employee
FROM gold.dim_employees;

-- find the employement status of employees
SELECT employment_status,
count(*) AS total_employee
FROM gold.dim_employees
GROUP BY employment_status
ORDER BY employment_status

            -- count of employees by department
SELECT department_id, department_name,
count(*) AS total_employees
FROM gold.dim_employees
GROUP BY department_id, department_name
ORDER BY department_id;

              --- find the oldest and youngest employees

SELECT 'youngest_employee' AS Measure_name,  MAX(birth_date) AS Measure,	DATEDIFF(Year, MAX(birth_date),  GETDATE()) AS Measure FROM  gold.dim_employees
UNION ALL
SELECT 'oldest_employee' AS Measure_name,  MIN(birth_date) AS Measure,		DATEDIFF(Year, MIN(birth_date), GETDATE()) AS Measure FROM  gold.dim_employees
UNION ALL 
SELECT 'recent_employee' AS Measure_name,  MAX(hire_date) AS Measure,		DATEDIFF(Year, MIN(hire_date),  GETDATE()) AS Measure FROM  gold.dim_employees
UNION ALL
SELECT 'longest_stayed_employee' AS Measure_name,  MIN(hire_date) AS Measure, DATEDIFF(Year, MIN(hire_date),  GETDATE()) AS Measure FROM  gold.dim_employees

              -- total salary
SELECT SUM(gross_salary) AS total_salary FROM gold.fact_salaries;
	            --- average salary
SELECT AVG(gross_salary) AS average_salary FROM gold.fact_salaries;


	            --- total bonus
SELECT SUM(bonus) AS total_bonus FROM gold.fact_salaries;
	           -- average bonus
SELECT AVG(bonus) AS average_bonus FROM gold.fact_salaries;

