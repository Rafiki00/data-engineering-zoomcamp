WITH trip_data AS (
    SELECT 
        pickup_datetime,
        fare_amount,
        service_type
    FROM `dbt_rllopis.fact_trips`
),

revenue_by_quarter AS (
    SELECT 
        d.year_quarter,
        t.service_type,
        SUM(t.fare_amount) AS total_revenue
    FROM trip_data t
    JOIN `dbt_rllopis.dim_date` d 
        ON DATE(t.pickup_datetime) = d.date_day
    GROUP BY d.year_quarter, t.service_type
)

SELECT 
    year_quarter,
    service_type,
    total_revenue,
    LAG(total_revenue) OVER (PARTITION BY service_type ORDER BY year_quarter) AS prev_quarter_revenue,
    SAFE_DIVIDE(total_revenue - LAG(total_revenue) OVER (PARTITION BY service_type ORDER BY year_quarter), 
                LAG(total_revenue) OVER (PARTITION BY service_type ORDER BY year_quarter)) AS revenue_growth,
    LAG(total_revenue, 4) OVER (PARTITION BY service_type ORDER BY year_quarter) AS prev_year_revenue,
    SAFE_DIVIDE(total_revenue - LAG(total_revenue, 4) OVER (PARTITION BY service_type ORDER BY year_quarter), 
                LAG(total_revenue, 4) OVER (PARTITION BY service_type ORDER BY year_quarter)) AS yoy_revenue_growth
FROM revenue_by_quarter
