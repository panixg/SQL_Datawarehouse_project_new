-- Description: This script is used to check the data in the gold tables.
-- Check for duplicate records in the silver table customer id
SELECT cst_id, COUNT(*) FROM
(
SELECT
    ci.cst_id,
    ci.cst_key,
    ci.cst_firstname,
    ci.cst_lastname,
    ci.cst_marital_status,
    ci.cst_gndr,
    ci.cst_create_date,
    ca.bdate,
    ca.gen,
    la.cntry
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
    ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
    ON ci.cst_key = la.cid
)
t GROUP BY cst_id HAVING COUNT(*) > 1;

--Data integration
--joining customer information from different sources creates repeated info in gender columns
    SELECT
        ci.cst_id,
        ci.cst_key,
        ci.cst_firstname,
        ci.cst_lastname,
        ci.cst_marital_status,
        ci.cst_gndr,
        ci.cst_create_date,
        ca.bdate,
        ca.gen,
        la.cntry
    FROM silver.crm_cust_info ci
    LEFT JOIN silver.erp_cust_az12 ca
        ON ci.cst_key = ca.cid
    LEFT JOIN silver.erp_loc_a101 la
        ON ci.cst_key = la.cid

--Check data consistency in both columns
SELECT DISTINCT
    ci.cst_gndr,
    ca.gen
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
    ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
    ON ci.cst_key = la.cid
ORDER BY 1,2
--There are inconsistencies. Supose the correct data comes from the CRM source:

SELECT DISTINCT
    ci.cst_gndr,
    ca.gen
    CASE WHEN ci.cst_gndr != 'Unknown' THEN ci.cst_gndr 
    ELSE COALESCE(ca.gen, 'Unknown')
    END AS new_gen
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
    ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
    ON ci.cst_key = la.cid
ORDER BY 1,2

--Integrate the CASE in the original SELECT, reorder the columns and add a surrogate key
--Create View
CREATE VIEW gold.dim_customer AS
SELECT
        ROW_NUMBER() OVER(ORDER BY ci.cst_id) AS customer_key,
        ci.cst_id AS customer_id,
        ci.cst_key AS customer_number,
        ci.cst_firstname AS first_name,
        ci.cst_lastname AS last_name,
        la.cntry AS country,
        ci.cst_marital_status AS marital_status,
        CASE WHEN ci.cst_gndr != 'Unknown' THEN ci.cst_gndr 
            ELSE COALESCE(ca.gen, 'Unknown')
        END AS gender,
        ca.bdate AS birth_date, 
        ci.cst_create_date AS create_date
               
    FROM silver.crm_cust_info ci
    LEFT JOIN silver.erp_cust_az12 ca
        ON ci.cst_key = ca.cid
    LEFT JOIN silver.erp_loc_a101 la
        ON ci.cst_key = la.cid

--Check the view
SELECT * FROM gold.dim_customer

SELECT
pn.prd_id,
pn.cat.id,
pn.prd_key,
pn.prd_nm,
pn.prd_cost,
pn.prd_line,
pn.prd_start_dt,
pn.prd_end_dt
FROM silver.crm_prd_info pn
