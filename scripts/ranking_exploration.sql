/*
===============================================================================
Ranking Analysis
===============================================================================
Purpose:
    - To rank items (e.g., products, customers) based on performance or other metrics.
    - To identify top performers or laggards.

SQL Functions Used:
    - Window Ranking Functions: RANK(), DENSE_RANK(), ROW_NUMBER(), TOP
    - Clauses: GROUP BY, ORDER BY
===============================================================================
*/

-- find best peforming department
SELECT department_id,rating,
COUNT (*) AS Number_of_employees
FROM gold.dim_performance
GROUP BY department_id,rating, department_id
HAVING rating > 3
ORDER BY department_id;



-- find worse peforming department
SELECT department_id,rating,
count(*) AS Number_of_employees
FROM gold.dim_performance
GROUP BY department_id,rating
HAVING rating < 3
ORDER BY department_id;


-- find manager with the best employees
SELECT manager_id,rating,
count(*) AS Number_of_employees
FROM gold.dim_performance
GROUP BY manager_id,rating
HAVING rating > 3
ORDER BY count(*) DESC

-- find manager with the worse employees
SELECT manager_id,rating,
count(*) AS Number_of_employees
FROM gold.dim_performance
GROUP BY manager_id,rating
HAVING rating < 3
ORDER BY count(*) DESC;

-- find TOP peforming employees
SELECT employee_id, first_name, last_name, rating
FROM gold.dim_performance
GROUP BY employee_id, first_name, last_name, rating
HAVING rating > 4
ORDER BY employee_id;

-- find bottom peforming employees
SELECT employee_id, first_name, last_name, rating
FROM gold.dim_performance
GROUP BY employee_id, first_name, last_name, rating
HAVING rating < 2
ORDER BY employee_id;

 

	-- max and min salary base on payment frequency
	SELECT Payment_frequency,
	MAX( gross_salary) AS Highest_salary, 
	MIN(gross_salary) AS Lowest_salary
	FROM gold.fact_salaries
	GROUP BY Payment_frequency;


	

	-- max and min salary by gender

	SELECT gender,	
	s.Payment_frequency,
	MAX(s.gross_salary) AS max_salary,
	MIN(S.GROSS_salary) min_salary
	FROM gold.dim_employees e
	LEFT JOIN gold.fact_salaries s
	ON e.employee_id = s.employee_id
	WHERE payment_frequency IS NOT NULL
	GROUP BY s.Payment_frequency, gender
	ORDER BY gender;

	-- max and min salary by department
	SELECT e.department_id, e.department_name,
	s.Payment_frequency,
	MAX(s.gross_salary) AS max_salary,
	MIN(S.GROSS_salary) AS min_salary
	FROM gold.dim_employees e
	LEFT JOIN gold.fact_salaries s
	ON e.employee_id = s.employee_id
	WHERE payment_frequency IS NOT NULL
	GROUP BY e.department_id, e.department_name, s.Payment_frequency
	ORDER BY e.department_id, e.department_name;
