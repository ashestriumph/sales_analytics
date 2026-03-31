-- ============================================================
-- SALES ANALYTICS — Advanced SQL Queries
-- Demonstrates: CTEs, window functions, partitioning awareness,
--               ranking, cumulative metrics, YoY comparisons
-- ============================================================

-- ── 1. Monthly revenue with MoM growth (window function) ─────────────────────
WITH monthly_revenue AS (
    SELECT
        d.year,
        d.month_num,
        d.month_name,
        SUM(f.net_amount)                                           AS revenue,
        SUM(f.gross_margin)                                         AS margin
    FROM warehouse.fact_sales f
    JOIN warehouse.dim_date d ON d.date_key = f.date_key
    GROUP BY d.year, d.month_num, d.month_name
)
SELECT
    year,
    month_num,
    month_name,
    ROUND(revenue, 2)                                               AS revenue,
    ROUND(margin, 2)                                                AS margin,
    ROUND(margin / NULLIF(revenue, 0) * 100, 2)                     AS margin_pct,
    ROUND(revenue - LAG(revenue) OVER (ORDER BY year, month_num), 2) AS mom_diff,
    ROUND(
        (revenue - LAG(revenue) OVER (ORDER BY year, month_num))
        / NULLIF(LAG(revenue) OVER (ORDER BY year, month_num), 0) * 100,
    2)                                                              AS mom_growth_pct
FROM monthly_revenue
ORDER BY year, month_num;


-- ── 2. Top 10 products by revenue with rank and running total ────────────────
WITH product_revenue AS (
    SELECT
        p.product_id,
        p.product_name,
        p.category,
        SUM(f.net_amount)   AS revenue,
        SUM(f.quantity)     AS units_sold,
        COUNT(DISTINCT f.order_id) AS order_count
    FROM warehouse.fact_sales f
    JOIN warehouse.dim_product p ON p.product_key = f.product_key
    GROUP BY p.product_id, p.product_name, p.category
)
SELECT
    RANK() OVER (ORDER BY revenue DESC)                             AS rank,
    product_id,
    product_name,
    category,
    ROUND(revenue, 2)                                               AS revenue,
    units_sold,
    order_count,
    ROUND(
        SUM(revenue) OVER (ORDER BY revenue DESC
                           ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
        / SUM(revenue) OVER () * 100,
    2)                                                              AS running_share_pct
FROM product_revenue
ORDER BY revenue DESC
LIMIT 10;


-- ── 3. Customer segmentation: RFM (Recency / Frequency / Monetary) ───────────
WITH rfm_raw AS (
    SELECT
        c.customer_id,
        c.customer_name,
        c.segment,
        MAX(d.full_date)                    AS last_purchase,
        COUNT(DISTINCT f.order_id)          AS frequency,
        ROUND(SUM(f.net_amount), 2)         AS monetary
    FROM warehouse.fact_sales f
    JOIN warehouse.dim_customer c ON c.customer_key = f.customer_key
    JOIN warehouse.dim_date     d ON d.date_key      = f.date_key
    GROUP BY c.customer_id, c.customer_name, c.segment
),
rfm_scored AS (
    SELECT *,
        CURRENT_DATE - last_purchase                                AS recency_days,
        NTILE(5) OVER (ORDER BY CURRENT_DATE - last_purchase ASC)  AS r_score,
        NTILE(5) OVER (ORDER BY frequency DESC)                     AS f_score,
        NTILE(5) OVER (ORDER BY monetary DESC)                      AS m_score
    FROM rfm_raw
)
SELECT
    customer_id,
    customer_name,
    segment,
    recency_days,
    frequency,
    monetary,
    r_score, f_score, m_score,
    (r_score + f_score + m_score)                                   AS rfm_total,
    CASE
        WHEN (r_score + f_score + m_score) >= 13 THEN 'Champions'
        WHEN (r_score + f_score + m_score) >= 10 THEN 'Loyal'
        WHEN (r_score + f_score + m_score) >= 7  THEN 'Potential'
        WHEN r_score <= 2                         THEN 'At Risk'
        ELSE 'Needs Attention'
    END                                                             AS customer_tier
FROM rfm_scored
ORDER BY rfm_total DESC;


-- ── 4. Year-over-Year revenue comparison by category ─────────────────────────
WITH yearly AS (
    SELECT
        d.year,
        p.category,
        ROUND(SUM(f.net_amount), 2)  AS revenue
    FROM warehouse.fact_sales f
    JOIN warehouse.dim_date    d ON d.date_key    = f.date_key
    JOIN warehouse.dim_product p ON p.product_key = f.product_key
    GROUP BY d.year, p.category
)
SELECT
    category,
    year,
    revenue,
    LAG(revenue) OVER (PARTITION BY category ORDER BY year)        AS prev_year_revenue,
    ROUND(
        (revenue - LAG(revenue) OVER (PARTITION BY category ORDER BY year))
        / NULLIF(LAG(revenue) OVER (PARTITION BY category ORDER BY year), 0) * 100,
    2)                                                              AS yoy_growth_pct
FROM yearly
ORDER BY category, year;


-- ── 5. Channel performance with penetration rate ──────────────────────────────
SELECT
    f.channel,
    COUNT(DISTINCT f.order_id)                                      AS orders,
    COUNT(DISTINCT f.customer_key)                                  AS unique_customers,
    ROUND(SUM(f.net_amount), 2)                                     AS revenue,
    ROUND(AVG(f.net_amount), 2)                                     AS avg_order_value,
    ROUND(SUM(f.net_amount) / SUM(SUM(f.net_amount)) OVER () * 100, 2) AS revenue_share_pct
FROM warehouse.fact_sales f
GROUP BY f.channel
ORDER BY revenue DESC;


-- ── 6. Partition pruning demo — only scans fact_sales_2024 ───────────────────
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT SUM(net_amount)
FROM warehouse.fact_sales
WHERE date_key BETWEEN 20240101 AND 20241231;
