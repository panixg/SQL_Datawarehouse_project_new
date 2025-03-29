--Change over time trends
SELECT
YEAR (order_date) AS order_year,
SUM(sales_amount) as total_sales,
COUNT(DISTINCT customer_key) as total_customers,
SUM (quantity) as total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY YEAR(order_date)
ORDER BY YEAR(order_date);

SELECT
MONTH (order_date) AS order_year,
SUM(sales_amount) as total_sales,
COUNT(DISTINCT customer_key) as total_customers,
SUM (quantity) as total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY MONTH(order_date)
ORDER BY MONTH(order_date);


SELECT
FORMAT(order_date, 'yyyy-MMM') AS order_date, --FORMAT function is used to format the date in the desired format, but changes the data type to string
SUM(sales_amount) as total_sales,
COUNT(DISTINCT customer_key) as total_customers,
SUM (quantity) as total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY FORMAT(order_date, 'yyyy-MMM')
ORDER BY DATETRUNC(month, order_date);

--Cumulative Analysis
--Aggregate the data progressively over time, partitioning by year
SELECT
    order_date,
    total_sales,
    SUM(total_sales) OVER (PARTITION BY YEAR(order_date)ORDER BY order_date) AS cumulative_sales
FROM(
    SELECT
        DATETRUNC(month, order_date) AS order_date, -- Truncate order_date to show only year and month
        SUM(sales_amount) as total_sales
    FROM gold.fact_sales
    WHERE order_date IS NOT NULL
    GROUP BY DATETRUNC(month, order_date) -- Group by the truncated year and month
) t
ORDER BY DATETRUNC(month, order_date); -- Order by the truncated year and month

--Performance Anlysis
--Comparing the current value to a target value

/* Analyze the yearly performance of products by comparing tyhir sales to both the avaerage sales
performance of the producto and the previous year's sales */

WITH yearly_product_sales AS(
SELECT
    YEAR(f.order_date) AS order_year,
    p.product_name,
    SUM(f.sales_amount) AS current_sales
FROM gold.fact_sales f
    LEFT JOIN gold.dim_product p
    ON f.product_key = p.product_key
WHERE f.order_date IS NOT NULL
GROUP BY YEAR(f.order_date), p.product_name
)
SELECT
order_year,
product_name,
current_sales,
AVG (current_sales) OVER (PARTITION BY product_name) AS avg_sales,
current_sales - AVG (current_sales) OVER (PARTITION BY product_name) AS diff_avg_sales,
CASE WHEN current_sales - AVG (current_sales) OVER (PARTITION BY product_name) > 0 THEN 'Above Average'
    WHEN current_sales - AVG (current_sales) OVER (PARTITION BY product_name) < 0 THEN 'Below Average'
    ELSE 'Average'
END AS avg_sales_performance,
LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) as prev_yr_sales,
current_sales - LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) AS diff_prev_yr_sales,
CASE WHEN current_sales - LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) > 0 THEN 'Increase'
    WHEN current_sales - LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) < 0 THEN 'Decrease'
    ELSE 'No Change'
END AS sales_performance
FROM yearly_product_sales
ORDER BY product_name

--Part to Whole (Proportion Analysis)
--Which category contributes the most to the total sales
WITH category_sales AS(
SELECT
category,
SUM(sales_amount) as total_sales
FROM gold.fact_sales f
    LEFT JOIN gold.dim_product p
    ON f.product_key = p.product_key
GROUP BY category)

SELECT
category,
total_sales,
SUM (total_sales) OVER () AS total_sales_all,
CONCAT(ROUND ((CAST(total_sales AS FLOAT) / SUM (total_sales) OVER ()) * 100, 2), '%') AS porcentage_of_total_sales
FROM category_sales
ORDER BY total_sales DESC;

--Data Segmentation
--Segment products into cost ranges and count how many products fall into each range
WITH product_segments AS(
SELECT
product_key,
product_name,
cost,
CASE WHEN cost < 100 THEN 'Below 100 Cost'
    WHEN cost >= 100 AND cost < 500 THEN '100-500 Cost'
    WHEN cost >= 500 AND cost < 1000 THEN '500-1000 Cost'
    ELSE 'Above 1000 Cost'
END AS cost_range    
FROM gold.dim_product)

SELECT
cost_range,
COUNT(product_key) as total_products
FROM product_segments
GROUP BY cost_range
ORDER BY total_products DESC;

/* Group custumers into three segments based on their spending behavior:
- VIP: Customers with at least 12 months of history and spending more than $5000
- Regular: Customers with at least 12 months of history and spending  $5000 or less
- New: Customers with less than 12 months of history 
Find the total number of customers in each segment */

WITH customer_segments AS(
SELECT
c.customer_key,
c.first_name,
c.last_name,
SUM(f.sales_amount) as total_spending,
MIN(f.order_date) AS first_order_date,
MAX(f.order_date) AS last_order_date,
DATEDIFF (month, MIN(f.order_date), MAX(f.order_date)) AS history_months
FROM gold.fact_sales f
    LEFT JOIN gold.dim_customer c
    ON f.customer_key = c.customer_key
GROUP BY c.customer_key , c.first_name, c.last_name
)

SELECT 
customer_segment,
COUNT(customer_key) as total_customers
FROM (
SELECT
customer_key,
first_name,
last_name,
total_spending,
CASE WHEN history_months >= 12 AND total_spending > 5000 THEN 'VIP'
    WHEN history_months >= 12 AND total_spending <= 5000 THEN 'Regular'
    ELSE 'New'
END AS customer_segment
FROM customer_segments) t
GROUP BY customer_segment
ORDER BY total_customers DESC;
