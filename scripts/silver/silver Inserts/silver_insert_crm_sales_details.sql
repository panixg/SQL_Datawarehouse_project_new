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

