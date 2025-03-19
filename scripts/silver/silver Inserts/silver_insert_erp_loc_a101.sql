-- Description: Inserts data into the silver.erp_loc_a101 table
-- =============================================
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