
INSERT INTO silver.crm_prd_info 
(
    prd_id,
    cat_id,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
)

SELECT
    prd_id,
    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, --to relate crm_prd_info table with erp_px_cat_g1v2 table, we need to create a new column cat_id for the product category
    SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key, --remove the first 6 characters from the prd_key column and create a new column prd_key
    prd_nm,
    ISNULL (prd_cost, 0) AS prd_cost, --replace NULL values with 0 in the prd_cost column
    CASE UPPER(TRIM (prd_line)) --standardize the values in the prd_line column
        WHEN 'M' THEN 'Mountain' 
        WHEN 'R' THEN 'Road'
        WHEN 'T' THEN 'Touring'
        WHEN 'S' THEN 'Other Sales'
        ELSE 'Unknown'
    END,
    CAST (prd_start_dt AS DATE) AS prd_start_dt, --convert the prd_start_dt column to DATE format
    CAST (LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt -- this will replace the prd_end_dt column with the next prd_start_dt value -1
FROM bronze.crm_prd_info
-- WHERE REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') NOT IN (SELECT DISTINCT id from bronze.erp_px_cat_g1v2) --to check if the cat_id column has values that are not in the id column of the erp_px_cat_g1v2 table'
-- WHERE SUBSTRING(prd_key, 7, LEN(prd_key)) NOT IN (SELECT sls_prd_key FROM bronze.crm_sales_details) --to check if the prd_key column has values that are not in the sls_prd_key column of the crm_sales_details table
-- this shows that there are 220 products in the crm_prd_info table that don't have any sales in the crm_sales_details table


