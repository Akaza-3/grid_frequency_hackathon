-- resources/sql/portfolio_stress_test.sql
-- Quarterly portfolio stress test. Blends per-customer exposure,
-- delinquency history and revolving utilisation into a single
-- stressed-loss view, keeping one row per customer (their largest
-- outstanding loan).

--dummy1
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
    WHERE l.loan_status IN ('Current', 'Late (31-120 days)', 'In Grace Period')
),

exposure AS (
    SELECT
        customer_id,
        SUM(CAST(out_prncp AS FLOAT64))  AS total_outstanding,
        SUM(CAST(loan_amnt AS FLOAT64))  AS total_originated,
        AVG(CAST(int_rate AS FLOAT64))   AS avg_int_rate,
        COUNT(*)                         AS loan_count
    FROM base
    GROUP BY customer_id
),

risk_history AS (
    SELECT
        customer_id,
        MAX(CAST(delinq_2yrs AS FLOAT64))   AS max_delinq,
        MAX(CAST(dti AS FLOAT64))           AS max_dti,
        MAX(CAST(revol_util AS FLOAT64))    AS max_revol_util,
        MIN(mths_since_last_delinq)         AS mths_since_last_delinq
    FROM base
    GROUP BY customer_id
),

stressed AS (
    SELECT
        b.*,
        e.total_outstanding,
        e.total_originated,
        e.avg_int_rate,
        e.loan_count,
        r.max_delinq,
        r.max_dti,
        r.max_revol_util,
        r.mths_since_last_delinq,
        e.total_outstanding * (1 + r.max_dti / 100) AS stressed_exposure,
        ROW_NUMBER() OVER (
            PARTITION BY b.customer_id
            ORDER BY CAST(b.out_prncp AS FLOAT64) DESC
        ) AS rn,
        NTILE(5) OVER (ORDER BY e.total_outstanding DESC) AS exposure_quintile
    FROM base b
    JOIN exposure e     ON b.customer_id = e.customer_id
    JOIN risk_history r ON b.customer_id = r.customer_id
    WHERE r.max_dti > 20 OR r.max_revol_util > 60
)

SELECT
    customer_id,
    customer_name,
    grade,
    emp_length,
    term,
    zip_code,
    loan_count,
    total_outstanding,
    stressed_exposure,
    exposure_quintile,
    max_dti,
    max_revol_util
FROM stressed
WHERE rn = 1
ORDER BY stressed_exposure DESC
