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
