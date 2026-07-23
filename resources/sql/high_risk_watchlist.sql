-- resources/sql/high_risk_watchlist.sql
--dummy comment
WITH base AS (
    SELECT
        l.*,
        c.customer_name,
        c.city,
        c.state,
        c.customer_segment,
        c.occupation,
        c.employer
    FROM `project-ff7c2ef5-8d88-401a-b86.loan_data.loan` l
    INNER JOIN `project-ff7c2ef5-8d88-401a-b86.loan_data.customers` c
        ON l.customer_id = c.customer_id
    WHERE l.loan_status = 'Current'
),

customer_risk_tier AS (
    SELECT
        customer_id,
        MAX(CAST(dti AS FLOAT64)) AS max_dti,
        MAX(CAST(revol_util AS FLOAT64)) AS max_revol_util,
        SUM(CAST(out_prncp AS FLOAT64)) AS total_outstanding,
        COUNT(*) AS loan_count
    FROM base
    GROUP BY customer_id
),

watchlist_flagged AS (
    SELECT
        b.*,
        t.max_dti,
        t.max_revol_util,
        t.total_outstanding,
        t.loan_count,
        ROW_NUMBER() OVER (
            PARTITION BY b.customer_id
            ORDER BY CAST(b.out_prncp AS FLOAT64) DESC
        ) AS rn
    FROM base b
    JOIN customer_risk_tier t ON b.customer_id = t.customer_id
    WHERE t.max_dti > 25 OR t.max_revol_util > 75
)

SELECT
    customer_id,
    customer_name,
    grade,
    sub_grade,
    out_prncp,
    max_dti,
    max_revol_util,
    total_outstanding,
    loan_count
FROM watchlist_flagged
WHERE rn = 1
ORDER BY total_outstanding DESC