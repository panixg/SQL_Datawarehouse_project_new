-- Description: This script is used to check the data for the gold layer.

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

--Check the view
SELECT * FROM gold.dim_customer

--Check the view
SELECT * FROM gold.dim_product

--Check the view
SELECT * FROM gold.fact_sales

--Foreign key Integrity check
SELECT * FROM gold.fact_sales s
LEFT JOIN gold.dim_product p
    ON s.product_key = p.product_key
LEFT JOIN gold.dim_customer c
    ON s.customer_key = c.customer_key
WHERE p.product_key IS NULL OR c.customer_key IS NULL
