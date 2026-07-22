-- resources/sql/customer_risk_dashboard.sql
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

portfolio_metrics AS (
    SELECT
        customer_id,
        COUNT(*) AS total_loans,
        SUM(CAST(loan_amnt AS FLOAT64)) AS total_amount,
        AVG(CAST(int_rate AS FLOAT64)) AS avg_interest
    FROM base
    GROUP BY customer_id
),

risk_metrics AS (
    SELECT
        customer_id,
        MAX(CAST(dti AS FLOAT64)) AS max_dti,
        MAX(CAST(revol_util AS FLOAT64)) AS max_revol_util,
        SUM(CAST(out_prncp AS FLOAT64)) AS total_outstanding
    FROM base
    GROUP BY customer_id
)

SELECT *
FROM base b
JOIN portfolio_metrics p ON b.customer_id = p.customer_id
JOIN risk_metrics r ON b.customer_id = r.customer_id
ORDER BY p.total_amount DESC