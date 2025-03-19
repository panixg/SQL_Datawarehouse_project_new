INSERT INTO silver.crm_cust_info (
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date
    )-- this will insert the cleaned data into the silver layer

    -- Description: This script is used to clean the data in the bronze layer of the data warehouse.
    SELECT
    cst_id,
    cst_key,
    TRIM (cst_firstname) as cst_firstname, --removes leading and trailing spaces
    TRIM (cst_lastname) as cst_lastname, --removes leading and trailing spaces

    CASE WHEN UPPER(TRIM (cst_marital_status)) = 'S' THEN 'Single' --standardize the values in the cst_marital_status column
        WHEN UPPER(TRIM (cst_marital_status)) = 'M' THEN 'Married'
        ELSE 'Unknown'
    END cst_marital_status, --standard  
    CASE WHEN UPPER(TRIM (cst_gndr)) = 'F' THEN 'Female' --standardize the values in the cst_gndr column
        WHEN UPPER(TRIM (cst_gndr)) = 'M' THEN 'Male'
        ELSE 'Unknown'
    END cst_gndr, --standard  
    cst_create_date

    FROM(
        SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last -- flag each record as the last record in the partition
        FROM bronze.crm_cust_info --this is a partition of every repeated cst_id and orders them by the most recent cst_create_date and numbers them as 1
        WHERE cst_id IS NOT NULL
        ) AS a
    WHERE a.flag_last = 1;

-- Load crm_prd_info

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

-- Load crm_sales_details
INSERT INTO silver.crm_sales_details 
(
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
)
SELECT
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL --if the sls_order_dt column has values that are less than or equal to 0 or have a length that is not equal to 8, then the value is NULL
        ELSE CAST (CAST (sls_order_dt AS VARCHAR) AS DATE) --else, then the value is casted to a date
    END AS sls_order_dt,
    CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
        ELSE CAST (CAST (sls_ship_dt AS VARCHAR) AS DATE)
    END AS sls_ship_dt,
    CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
        ELSE CAST (CAST (sls_due_dt AS VARCHAR) AS DATE)
    END AS sls_due_dt,     
    --Sales must be equal to quantity * price. Negative, zero or Null values are not allowed.
    /*
    Rules:
    If sales is negative, zero or Null, derive it using Quantity * Price
    If Price is zero or null, calculate it using Sales / Quantity
    If Price is negative, convert it to positive 
    */
    CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_price)
        THEN sls_quantity * ABS(sls_price)
        ELSE sls_sales 
    END AS sls_sales,
    sls_quantity,
    CASE WHEN sls_price IS NULL OR sls_price <= 0
        THEN sls_sales / NULLIF(sls_quantity,0)  -- if sls_quantity is 0, then NULL
        ELSE sls_price
    END AS sls_price
    
FROM bronze.crm_sales_details

-- Load erp_cust_az12
INSERT INTO silver.erp_cust_az12
(
    cid,
    bdate,
    gen
)

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

-- Load erp_loc_a101
INSERT INTO silver.erp_loc_a101
(
    cid,
    cntry
)

SELECT
    REPLACE (cid, '-', '') AS cid,
    CASE 
        WHEN TRIM(cntry) IN ('USA', 'US') THEN 'United States'
        WHEN TRIM(cntry) = 'DE' THEN 'Germany'
        WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'Unknown'
        ELSE TRIM(cntry)
    END AS cntry
FROM bronze.erp_loc_a101

-- Load erp_px_cat_g1v2
INSERT INTO silver.erp_px_cat_g1v2
(
    id,
    cat,
    subcat,
    maintenance
)

SELECT
    id,
    TRIM(cat) AS cat,
    TRIM(subcat) AS subcat,
    TRIM(maintenance) AS maintenance
FROM bronze.erp_px_cat_g1v2