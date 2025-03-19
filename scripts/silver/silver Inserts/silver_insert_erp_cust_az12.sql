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