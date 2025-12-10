/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/
 
CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '================================================';
		PRINT 'Loading Silver Layer';
		PRINT '================================================';

		
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.employees';
		TRUNCATE TABLE silver.employees;
        INSERT INTO silver.employees (
                employee_id,
                first_name,
                last_name,
                gender,
                birth_date,
                hire_date,
                email,
                phone,
                city,
                states,
                zip,
                country,
                department_id,
                job_title,
                employment_status,
                full_time,
                manager_id,
                department_name,
                locations
		                            )
                SELECT DISTINCT employee_id ,
                UPPER(LEFT(first_name, 1)) + LOWER(SUBSTRING(first_name, 2, LEN(first_name))) AS first_name,
                TRANSLATE((UPPER(LEFT(last_name, 1)) + LOWER(SUBSTRING(last_name, 2, LEN(last_name)))), '0123456789', '          ') AS last_name,
                gender,
                birth_date,
                hire_date,
                CASE WHEN email IS NULL  THEN 
                        LOWER(first_name) + '.' + LOWER(last_name) + RIGHT(CAST(employee_id AS NVARCHAR(5)), 2) + '@example.com'
                        ELSE email
                END AS email,
                phone,
                city,
                states,
                zip,
                country,
                department_id,
                job_title,
                employment_status,
                employment_type,
                manager_id,
                department_name,
                locations
                
        FROM(
        SELECT DISTINCT
         e.employee_id,
        p.employee_id AS employee_id_p,
        CASE WHEN TRIM(e.first_name) IS NULL THEN  PARSENAME(REPLACE(LEFT(e.email, CHARINDEX('@', e.email)-1), '.', '.'), 2) 
	        ELSE e.first_name
        END AS first_name,
        CASE WHEN TRIM(e.last_name) IS NULL THEN  PARSENAME(REPLACE(LEFT(e.email, CHARINDEX('@', e.email)-1), '.', '.' ),1)-- comeback to rmove the numbers
	        ELSE e.last_name
        END AS last_name,
        CASE WHEN UPPER(TRIM(e.gender)) IN ('F', 'FEMALE') THEN 'Female'  -- standardize gender
	         WHEN UPPER(TRIM(e.gender)) IN ('M', 'MALE') THEN 'Male'
	         ELSE 'Other'
        END AS gender,
        CASE WHEN e.birth_date > GETDATE() THEN NULL
	         ELSE e.birth_date
        END AS birth_date,-- Set future birthdates to NULL
        CASE WHEN e.hire_date IS NULL THEN DATEADD(YEAR, -1, p.review_date) -- employees are normally review a year after employment and avoid review date comming before hire date
             ELSE e.hire_date
        END AS hire_date, 
       CASE WHEN e.email IS NULL  THEN 
            LOWER(e.first_name) + '.' + LOWER(e.last_name) + RIGHT(CAST(e.employee_id AS NVARCHAR(5)), 2) + '@example.com'
     ELSE e.email
    END AS email,
        CAST( ISNULL(e.phone,'n/a') AS NVARCHAR) AS phone,
        COALESCE(e.city, 'Unknown') AS city,
        COALESCE (e.states, 'Unknown') AS states,
        CAST(ISNULL(e.zip, 'n/a') AS NVARCHAR) AS zip,
        COALESCE(e.country, 'Ghana') AS country,
         CASE -- fill in null values for departments IDs
            WHEN e.department_id IS NULL AND e.job_title IN ('Software Engineer','Senior Software Engineer','Backend Developer','Frontend Developer','DevOps Engineer','Engineering Manager')
                THEN 1
            WHEN e.department_id IS NULL AND e.job_title IN ('HR Specialist','HR Manager','HRBP','Recruiter','Talent Acquisition')
                THEN 2
            WHEN e.department_id IS NULL AND e.job_title IN ('Finance Manager','Finance Analyst','Accountant','Controller','Treasurer')
                THEN 3
            WHEN e.department_id IS NULL AND e.job_title IN ('Account Executive','Sales Manager','Sales Associate','Business Development')
                THEN 4
            WHEN e.department_id IS NULL AND e.job_title IN ('Marketing Coordinator','Marketing Manager','Content Strategist','SEO Specialist')
                THEN 5
            WHEN e.department_id IS NULL AND e.job_title IN ('Customer Success Manager','Support Agent','Support Manager','Technical Support')
                THEN 6
            WHEN e.department_id IS NULL AND e.job_title IN ('Legal Assistant','Legal Counsel','Compliance Officer')
                THEN 7
            WHEN e.department_id IS NULL AND e.job_title IN ('Operations Manager','Operations Analyst','Logistics Coordinator')
                THEN 8
            WHEN e.department_id IS NULL AND e.job_title IN ('Product Owner','Product Manager','Business Analyst','UX Designer')
                THEN 9
            WHEN e.department_id IS NULL AND e.job_title IN ('IT Support','IT Manager','Network Engineer','Systems Administrator')
                THEN 10
            WHEN e.department_id IS NULL AND e.job_title IN ('Procurement Officer','Sourcing Specialist','Buying Manager')
                THEN 11
            WHEN e.department_id IS NULL AND e.job_title IN ('Research Scientist','Lab Technician','R&D Engineer')
                THEN 12
            ELSE e.department_id  -- keep existing values
        END AS department_id,
         CASE -- correcting spelling mistakes in Job Titles by departments
            -- Engineering / Software
            WHEN e.job_title IN ('Software Engineer','Software Enigneer') THEN 'Software Engineer'
            WHEN e.job_title IN ('Senior Software Engineer','Senio rSoftware Engineer','Senior Software Egnineer','Senior Software Engiener') THEN 'Senior Software Engineer'
            WHEN e.job_title IN ('Backend Developer','Backen dDeveloper','Backend Develoepr','BackendD eveloper','Backned Developer','Baceknd Developer') THEN 'Backend Developer'
            WHEN e.job_title IN ('Frontend Developer','Frontend Develoepr') THEN 'Frontend Developer'
            WHEN e.job_title IN ('Engineering Manager','Engineering Mnaager','Enigneering Manager') THEN 'Engineering Manager'
            WHEN e.job_title = 'DevOps Engineer' THEN 'DevOps Engineer'

            -- HR
            WHEN e.job_title IN ('HR Specialist','HR Specialsit','HR pSecialist','HRPB','HRBP') THEN 'HR Specialist'
            WHEN e.job_title IN ('HR Manager','HR Mangaer') THEN 'HR Manager'
            WHEN e.job_title IN ('Recruiter','Recruitre') THEN 'Recruiter'
            WHEN e.job_title IN ('Talent Acquisition','Talent Acquisiiton','Talent Acquisitoin','Talent Acquistiion','Talent Acuqisition','Talent cAquisition','aTlent Acquisition') THEN 'Talent Acquisition'

            -- Finance
            WHEN e.job_title IN ('Accountant','Acconutant','Accountatn','Accountatn') THEN 'Accountant'
            WHEN e.job_title IN ('Controller','Contorller','Contrloler','Contorller') THEN 'Controller'
            WHEN e.job_title IN ('Finance Analyst','Finacne Analyst','Fniance Analyst') THEN 'Finance Analyst'
            WHEN e.job_title IN ('Finance Manager','Finance Maanger','FinanceM anager','iFnance Manager') THEN 'Finance Manager'
            WHEN e.job_title IN ('Treasurer','rTeasurer','Treasurre') THEN 'Treasurer'

            -- Sales
            WHEN e.job_title IN ('Account Executive','Account Executiev') THEN 'Account Executive'
            WHEN e.job_title IN ('Sales Manager','Sale sManager','Sales Maanger','Sales Mnaager','Sales aMnager','SalesM anager','Sale sAssociate','Sales Associate') THEN 'Sales Manager'
            WHEN e.job_title = 'Business Development' OR e.job_title IN ('Business Deevlopment','Business Developemnt') THEN 'Business Development'

            -- Marketing
            WHEN e.job_title IN ('Marketing Coordinator','Marketign Coordinator','Marketing Coordiantor','Marketing Coordinaotr','Marketin gCoordinator') THEN 'Marketing Coordinator'
            WHEN e.job_title IN ('Marketing Manager','Marketign Manager','Marketin gCoordinator') THEN 'Marketing Manager'
            WHEN e.job_title IN ('Content Strategist','Contnet Strategist') THEN 'Content Strategist'
            WHEN e.job_title IN ('SEO Specialist','SEO Speciailst','SOE Specialist') THEN 'SEO Specialist'

            -- Support
            WHEN e.job_title IN ('Customer Success Manager','Customer Succses Manager','Customer Scucess Manager') THEN 'Customer Success Manager'
            WHEN e.job_title IN ('Support Agent','Supprot Agent') THEN 'Support Agent'
            WHEN e.job_title IN ('Support Manager','Support Managre','uSpport Manager') THEN 'Support Manager'
            WHEN e.job_title IN ('Technical Support','Technical Supoprt') THEN 'Technical Support'

            -- Legal / Compliance
            WHEN e.job_title IN ('Compliance Officer','Compliacne Officer','Compliance fOficer','Compliance Offcier','Compliance Offiecr','Complinace Officer') THEN 'Compliance Officer'
            WHEN e.job_title IN ('Legal Assistant','Legal Asisstant','Legal Assistnat','Legal Asssitant','LegalA ssistant') THEN 'Legal Assistant'
            WHEN e.job_title IN ('Legal Counsel','Legal Conusel','Legal Counesl','Legal oCunsel','Legla Counsel') THEN 'Legal Counsel'

            -- Operations / Logistics
            WHEN e.job_title IN ('Operations Manager','Opeartions Manager','Operatinos Manager','Operations Maanger','Operations Mangaer','Operations Mnaager','pOerations Manager') THEN 'Operations Manager'
            WHEN e.job_title IN ('Operations Analyst','Operations Analyts','Opreations Analyst','pOerations Analyst') THEN 'Operations Analyst'
            WHEN e.job_title IN ('Logistics Coordinator','Logistcis Coordinator','Logistisc Coordinator','Logitsics Coordinator','Logistcis Coordinator') THEN 'Logistics Coordinator'

            -- Product / UX
            WHEN e.job_title IN ('Product Owner','Prdouct Owner','Prdouct Owner','Prdouct Owner') THEN 'Product Owner'
            WHEN e.job_title IN ('Product Manager','Product aMnager','Product Maanger','Produtc Manager','Product aMnager') THEN 'Product Manager'
            WHEN e.job_title IN ('Business Analyst','Buisness Analyst','Busienss Analyst','Business Anaylst','Business nAalyst','Busienss Analyst') THEN 'Business Analyst'
            WHEN e.job_title IN ('UX Designer','UX Desigenr','UX Desinger','UX Desinger') THEN 'UX Designer'

            -- IT
            WHEN e.job_title IN ('IT Support','I TSupport','IT Spuport','ITS upport') THEN 'IT Support'
            WHEN e.job_title IN ('IT Manager','IT aMnager','IT Maanger','IT Maanger') THEN 'IT Manager'
            WHEN e.job_title IN ('Network Engineer','Network Enigneer','Network nEgineer','Newtork Engineer') THEN 'Network Engineer'
            WHEN e.job_title IN ('Systems Administrator','Systems Administrtaor','Systems Admniistrator') THEN 'Systems Administrator'

            -- Procurement
            WHEN e.job_title IN ('Procurement Officer','Procuremen tOfficer','Procurement Offcier','ProcurementO fficer','Procuremnet Officer','Porcurement Officer') THEN 'Procurement Officer'
            WHEN e.job_title IN ('Sourcing Specialist','Sourcnig Specialist','Sourcnig Specialist') THEN 'Sourcing Specialist'
            WHEN e.job_title = 'Buying Manager' THEN 'Buying Manager'

            -- Research
            WHEN e.job_title IN ('Research Scientist','Researc hScientist','eRsearch Scientist') THEN 'Research Scientist'
            WHEN e.job_title IN ('Lab Technician') THEN 'Lab Technician'
            WHEN e.job_title IN ('R&D Engineer','R&D nEgineer','R&DE ngineer','RD& Engineer','R&D nEgineer','RD& Engineer') THEN 'R&D Engineer'
            WHEN e.job_title IS NULL THEN 'Unknow'
            ELSE e.job_title
        END AS job_title,
        ISNULL (e.employment_status, 'Unknown') AS employment_status,  
        CASE WHEN UPPER(TRIM(e.full_time)) IN ('TRUE', 'Full Time') THEN 'Full Time' -- standardize 
	         WHEN UPPER(TRIM(e.full_time)) IN ('FALSE', 'Part Time') THEN 'Part Time'
	         ELSE 'Unknown'
        END AS employment_type,
         CASE WHEN  e.manager_id IS NULL THEN  p.reviewer_id -- reviewer id is same as manager_id
             WHEN  e.manager_id IS NULL  AND p.reviewer_id IS NULL THEN  'n/a'
        ELSE e.manager_id

        END AS manager_id,
        d.department_name,
        d.locations,
        p.reviewer_id,
        p.review_date
        FROM bronze.employees e
        LEFT JOIN  bronze.departments d
            ON e.department_id = d.department_id
        LEFT JOIN bronze.performance_reviews p
            ON e.employee_id = p.employee_id
        WHERE e.employee_id IS NOT NULL
         )t
        

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> --------------------------------------------------------------------';

       
        -- loading  silver salaries table
        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.salaries';
		TRUNCATE TABLE silver.salaries;
        INSERT INTO silver .salaries(

        employee_id,
        salary_gross,
        currency,
        pay_frequently,
        bonus,
        effective_date
        )
    SELECT
    
        employee_id,
        salary_gross,
        currency,
        pay_frequently,
        bonus,
        data_date
          
        
        FROM(   
         SELECT DISTINCT
        s.employee_id, 
        ISNULL(s.pay_frequently, 'Unknown') AS pay_frequently,
        ISNULL(s.currency, 'GHS') AS currency,
         CASE 
                -- Fill salary_gross if NULL using department averages
                WHEN s.salary_gross IS NULL AND e.department_id = 1 THEN 8502  --  Engineering avg
                WHEN s.salary_gross IS NULL AND e.department_id = 2 THEN 6156   -- Human Resources avg
                WHEN s.salary_gross IS NULL AND e.department_id = 3 THEN 6800   -- Finance
                WHEN s.salary_gross IS NULL AND e.department_id = 4 THEN 6783   -- Sales avg
                WHEN s.salary_gross IS NULL AND e.department_id = 5 THEN 5974   -- Marketing avg
                WHEN s.salary_gross IS NULL AND e.department_id = 6 THEN 6734   -- Customer Support avg
                WHEN s.salary_gross IS NULL AND e.department_id = 7 THEN 3955  --  Legal avg
                WHEN s.salary_gross IS NULL AND e.department_id = 8 THEN 7370  --  Operation avg
                WHEN s.salary_gross IS NULL AND e.department_id = 9 THEN 7061  --  Product avg
                WHEN s.salary_gross IS NULL AND e.department_id = 10 THEN 6423  -- IT avg
                WHEN s.salary_gross IS NULL AND e.department_id = 11 THEN 7011  -- Procurement avg
                WHEN s.salary_gross IS NULL AND e.department_id = 12 THEN 6565 --  R&D avg
                ELSE s.salary_gross
            END AS salary_gross, 
        s.bonus,
       ISNULL( s.effective_date, '2025-12-02') data_date
    FROM bronze.salaries s
    LEFT JOIN bronze.employees e
        ON s.employee_id = e.employee_id
    LEFT JOIN bronze.performance_reviews p
        ON   s.employee_id = p.employee_id
    WHERE e.department_id IS NOT NULL)t
     
                SET @end_time = GETDATE();
                PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
                PRINT '>> --------------------------------------------------------------------';


-- loading data into silver department
        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.departments';
		TRUNCATE TABLE silver.departments;
        INSERT INTO silver.departments(
                    department_id,
                    department_name,
                    locations
                                       )
         SELECT 
                    department_id,
                    department_name,
                    locations
         FROM bronze.departments
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> --------------------------------------------------------------------';



        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.performance_reviews';
		TRUNCATE TABLE silver.performance_reviews;
        INSERT INTO silver.performance_reviews(
                    review_id,
                    employee_id,
                    reviewer_id,
                    review_date,
                    rating,
                    goals_met,
                    promotion_recommendation,
                    comments
         
        )
        SELECT DISTINCT
        p.review_id,
        e.employee_id, 
        CASE WHEN  p.reviewer_id IS NULL THEN e.manager_id -- reviewer id is same as manager_id
        ELSE reviewer_id
        END AS reviewer_id,
        CASE WHEN review_date < hire_date THEN DATEADD(YEAR, 1, hire_date) -- push review date forward by 1 year
            WHEN review_date  IS NULL THEN DATEADD(YEAR, 1, hire_date)
            ELSE review_date
        END AS review_date,
        CASE WHEN p.rating IS NULL AND p.comments = 'Needs improvement in key areas.'			THEN  1  -- fill out missing rating base on comments
	         WHEN p.rating IS NULL AND p.comments = 'Needs improvement in key areas.'			THEN  2
	         WHEN p.rating IS NULL AND p.comments = 'Meets expectations; room for growth.'		THEN  3
	         WHEN p.rating IS NULL AND p.comments = 'Strong performance; exceeds expectations.'	THEN  4
	         WHEN p.rating IS NULL AND p.comments = 'Outstanding contributor; top performer.'	THEN  5
	         ELSE p.rating
	         END AS rating,
             CASE WHEN UPPER(TRIM(p.goals_met)) IN ('TRUE', 'YES') THEN 'YES' --standardize field
	         WHEN UPPER(TRIM(p.goals_met)) IN ('FALSE', 'NO') THEN 'NO'
             WHEN p.goals_met IS NULL THEN 'n/a'
	         ELSE goals_met
        END AS goals_met,
        CASE WHEN UPPER(TRIM( p.promotion_recommendation)) IN ('TRUE', 'YES') THEN 'YES' -- standardize fields
	         WHEN UPPER(TRIM( p.promotion_recommendation)) IN ('FALSE', 'NO') THEN 'NO'
	         ELSE  'NO'
        END AS promotion_recommendation,
        CASE WHEN p.comments IS NULL AND p.rating = 1 THEN 'Needs improvement in key areas.'	-- fill out missing comments		
	         WHEN p.comments IS NULL AND p.rating = 2 THEN 'Needs improvement in key areas.'	
	         WHEN p.comments IS NULL AND p.rating = 3 THEN 'Meets expectations; room for growth.'	
	         WHEN p.comments IS NULL AND p.rating = 4 THEN 'Strong performance; exceeds expectations.'
	         WHEN p.comments IS NULL AND p.rating = 5 THEN 'Outstanding contributor; top performer.'
	         ELSE comments
	         END AS comments
     FROM bronze.employees e
    LEFT JOIN bronze.performance_reviews p
         ON e.employee_id = p.employee_id
     WHERE p.review_id IS NOT NULL  -- filter out employee who did not take part in the performance review
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> --------------------------------------------------------------------';

		SET @batch_end_time = GETDATE();
		PRINT '=========================================='
		PRINT 'Loading Silver Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=========================================='
		
	END TRY
	BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING SILVER LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH
END


