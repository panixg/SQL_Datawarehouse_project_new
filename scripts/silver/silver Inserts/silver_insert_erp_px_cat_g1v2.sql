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