-- Reference analytical queries against the star schema

-- 1. Top 10 pickup zones by trip volume
SELECT pickup_zone, COUNT(*) AS total_trips
FROM fact_trips
GROUP BY pickup_zone
ORDER BY total_trips DESC
LIMIT 10;

-- 2. Average fare by borough
SELECT pickup_borough, ROUND(AVG(total_amount), 2) AS avg_fare
FROM fact_trips
GROUP BY pickup_borough
ORDER BY avg_fare DESC;

-- 3. Revenue by month
SELECT d.year, d.month, SUM(f.total_amount) AS revenue
FROM fact_trips f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY d.year, d.month
ORDER BY d.year, d.month;

-- 4. Top 3 pickup zones per borough (window function)
SELECT *
FROM (
    SELECT
        pickup_borough,
        pickup_zone,
        COUNT(*) AS trips,
        DENSE_RANK() OVER (PARTITION BY pickup_borough ORDER BY COUNT(*) DESC) AS rank
    FROM fact_trips
    GROUP BY pickup_borough, pickup_zone
) ranked
WHERE rank <= 3;

-- 5. Weekend vs weekday trip volume and average fare
SELECT
    d.is_weekend,
    COUNT(*) AS trips,
    ROUND(AVG(f.total_amount), 2) AS avg_fare
FROM fact_trips f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY d.is_weekend;

-- 6. Payment type distribution
SELECT p.payment_description, COUNT(*) AS trips, ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct
FROM fact_trips f
JOIN dim_payment p ON f.payment_key = p.payment_key
GROUP BY p.payment_description
ORDER BY trips DESC;

-- 7. Month-over-month revenue growth (window function: LAG)
SELECT
    year, month, revenue,
    ROUND(100.0 * (revenue - LAG(revenue) OVER (ORDER BY year, month)) / LAG(revenue) OVER (ORDER BY year, month), 1) AS mom_growth_pct
FROM (
    SELECT d.year, d.month, SUM(f.total_amount) AS revenue
    FROM fact_trips f
    JOIN dim_date d ON f.date_key = d.date_key
    GROUP BY d.year, d.month
) monthly
ORDER BY year, month;
