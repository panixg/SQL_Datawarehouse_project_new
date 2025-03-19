--Checks in cst_info table
    -- check for duplicates in the cst_id column
    SELECT
    cst_id,
    COUNT(*)
    FROM bronze.crm_cust_info
    GROUP BY cst_id
    HAVING COUNT(*) > 1 OR cst_id IS NULL;

    --check for unwanted spaces
    SELECT cst_firstname
    FROM bronze.crm_cust_info 
    WHERE cst_firstname != TRIM (cst_firstname)

    SELECT cst_lastname
    FROM bronze.crm_cust_info 
    WHERE cst_lastname != TRIM (cst_lastname)

    --Data Standardization & Consistency
    SELECT DISTINCT cst_marital_status
    FROM bronze.crm_cust_info;

    SELECT DISTINCT cst_gndr
    FROM bronze.crm_cust_info;
--Checks in cst_prd_info table
    -- check for duplicates in the prd_id column
    SELECT
    prd_id,
    COUNT(*)
    FROM bronze.crm_prd_info
    GROUP BY prd_id
    HAVING COUNT(*) > 1 OR prd_id IS NULL;
    
    --check for unwanted spaces 
    SELECT prd_nm
    from bronze.crm_prd_info
    WHERE prd_nm != TRIM (prd_nm)

    --check for NULL or Negative Numbers
    SELECT prd_cost
    FROM bronze.crm_prd_info
    WHERE prd_cost IS NULL OR prd_cost < 0;

    --Data Standardization & Consistency
    SELECT DISTINCT prd_line
    FROM bronze.crm_prd_info; --this column should only have values 'Mountain', 'Road', 'Touring', 'Other Sales', 'Unknown'
    
    --check for invalid dates orders
    SELECT *
    FROM bronze.crm_prd_info
    WHERE prd_start_dt > prd_end_dt; --there are 200 invalid dates in the crm_prd_info table