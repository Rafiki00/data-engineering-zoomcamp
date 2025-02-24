{{
    config(
        materialized='table'
    )
}}

WITH trip_dates AS (
    -- Join trips with dim_date to extract year and quarter
    SELECT 
        t.service_type,
        d.year,
        d.quarter,
        SUM(t.total_amount) AS quarterly_revenue
    FROM {{ ref('fact_trips') }} t
    INNER JOIN {{ ref('dim_date') }} d 
        ON DATE(t.pickup_datetime) = d.date_day  -- Ensure date match
    GROUP BY t.service_type, d.year, d.quarter
),

quarterly_growth AS (
    -- Calculate Year-over-Year (YoY) Growth
    SELECT 
        td.*,
        LAG(td.quarterly_revenue) OVER (
            PARTITION BY td.service_type, td.quarter 
            ORDER BY td.year
        ) AS prev_year_revenue,
        ROUND(
            SAFE_DIVIDE(td.quarterly_revenue - LAG(td.quarterly_revenue) OVER (
                PARTITION BY td.service_type, td.quarter 
                ORDER BY td.year
            ), LAG(td.quarterly_revenue) OVER (
                PARTITION BY td.service_type, td.quarter 
                ORDER BY td.year
            )) * 100, 2
        ) AS yoy_growth_percentage
    FROM trip_dates td
)

SELECT * FROM quarterly_growth
ORDER BY service_type, year, quarter;
