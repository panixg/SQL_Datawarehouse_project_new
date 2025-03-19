-- Checks in silver.crm_cust_info table
    -- check for duplicates in the cst_id column
    SELECT
    cst_id,
    COUNT(*)
    FROM silver.crm_cust_info
    GROUP BY cst_id
    HAVING COUNT(*) > 1 OR cst_id IS NULL;

    --check for unwanted spaces
    SELECT cst_firstname
    FROM silver.crm_cust_info 
    WHERE cst_firstname != TRIM (cst_firstname)

    SELECT cst_lastname
    FROM silver.crm_cust_info 
    WHERE cst_lastname != TRIM (cst_lastname)

    --Data Standardization & Consistency
    SELECT DISTINCT cst_marital_status
    FROM silver.crm_cust_info;

    SELECT DISTINCT cst_gndr
    FROM silver.crm_cust_info;

    SELECT * FROM silver.crm_cust_info;
-- Checks in cst_prd_info table
    -- check for duplicates in the prd_id column
    -- expected: no duplicates
    SELECT
    prd_id,
    COUNT(*)
    FROM silver.crm_prd_info
    GROUP BY prd_id
    HAVING COUNT(*) > 1 OR prd_id IS NULL;
    
    --check for unwanted spaces 
    SELECT prd_nm
    from silver.crm_prd_info
    WHERE prd_nm != TRIM (prd_nm)

    --check for NULL or Negative Numbers
    SELECT prd_cost
    FROM silver.crm_prd_info
    WHERE prd_cost IS NULL OR prd_cost < 0;

    --Data Standardization & Consistency
    SELECT DISTINCT prd_line
    FROM silver.crm_prd_info; --this column should only have values 'Mountain', 'Road', 'Touring', 'Other Sales', 'Unknown'
    
    --check for invalid dates orders
    SELECT *
    FROM silver.crm_prd_info
    WHERE prd_start_dt > prd_end_dt;
-- Checks in cst_sales_details table
    -- Check for invalid dates
    SELECT
    NULLIF (sls_order_dt, 0) AS sls_order_dt -- if null then 0
    FROM bronze.crm_sales_details
    WHERE sls_order_dt <= 0                 -- if less than or equal to 0 or lenght not equal to 8
    OR LEN(sls_order_dt) != 8 
    OR sls_order_dt > 20250101
    OR sls_order_dt < 20000101;

    --Check if the order date is greater than the ship date
    SELECT
    *
    FROM silver.crm_sales_details
    WHERE sls_order_dt > sls_ship_dt OR sls_ship_dt > sls_due_dt;

    --Check sls_sales values
    SELECT DISTINCT
    sls_sales,
    sls_quantity,
    sls_price 
    FROM silver.crm_sales_details
    WHERE sls_sales != (sls_quantity * sls_price)
    OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
    OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
    ORDER BY sls_sales, sls_quantity, sls_price;
-- Checks in erp_cust_az12 table
    SELECT
    CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) -- if cid starts with 'NAS' then remove 'NAS'
        ELSE cid
    END AS cid,
    CASE WHEN bdate > GETDATE() THEN NULL -- if bdate is greater than today's date then NULL
        ELSE bdate
    END AS bdate,
    CASE WHEN UPPER(TRIM(GEN)) IN ('F', 'FEMALE') THEN 'Female'
        WHEN UPPER(TRIM(GEN)) IN ('M', 'MALE') THEN 'Male'
        ELSE 'Unknown'
    END AS gen
    FROM bronze.erp_cust_az12

    SELECT DISTINCT
    bdate
    FROM silver.erp_cust_az12
    WHERE bdate > GETDATE() OR bdate < '1924-01-01';

    SELECT DISTINCT 
    gen
    FROM silver.erp_cust_az12
-- Checks in erp_loc_a101
    SELECT
    REPLACE (cid, '-', '') AS cid,
    CASE 
        WHEN TRIM(cntry) IN ('USA', 'US') THEN 'United States'
        WHEN TRIM(cntry) = 'DE' THEN 'Germany'
        WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'Unknown'
        ELSE TRIM(cntry)
    END AS cntry
    FROM bronze.erp_loc_a101

    --Data Standardization & Consistency
    SELECT DISTINCT cntry
    FROM silver.erp_loc_a101
    ORDER BY cntry;

    SELECT * FROM silver.erp_loc_a101;
-- Checks in erp_px_cat_g1v2
    SELECT
    id,
    cat,
    subcat,
    maintenance
    FROM bronze.erp_px_cat_g1v2;

    --check for unwanted spaces
    SELECT * FROM bronze.erp_px_cat_g1v2
    WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance);

    SELECT DISTINCT
    cat
    FROM bronze.erp_px_cat_g1v2
    ORDER BY cat;

    SELECT DISTINCT
    maintenance
    FROM bronze.erp_px_cat_g1v2
    ORDER BY maintenance;

    SELECT * FROM silver.erp_px_cat_g1v2

  