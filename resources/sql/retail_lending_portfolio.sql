-- resources/sql/retail_lending_portfolio.sql
WITH loan_data AS (
    SELECT *
    FROM `project-ff7c2ef5-8d88-401a-b86.loan_data.loan`
),

customer_data AS (
    SELECT *
    FROM `project-ff7c2ef5-8d88-401a-b86.loan_data.customers`
),

joined_data AS (
    SELECT
        l.*,
        c.customer_name,
        c.city,
        c.state,
        c.customer_segment,
        c.occupation,
        c.employer
    FROM loan_data l
    INNER JOIN customer_data c
        ON l.customer_id = c.customer_id
),

current_loans AS (
    SELECT *
    FROM joined_data
    WHERE loan_status = 'Current'
),

verified_loans AS (
    SELECT *
    FROM current_loans
    WHERE verification_status IN (
        'Verified',
        'Source Verified'
    )
),

income_filter AS (
    SELECT *
    FROM verified_loans
    WHERE CAST(annual_inc AS FLOAT64) > 50000
),

dti_filter AS (
    SELECT *
    FROM income_filter
    WHERE CAST(dti AS FLOAT64) < 30
),

grade_filter AS (
    SELECT *
    FROM dti_filter
    WHERE grade IN ('A','B','C','D')
),

utilization_filter AS (
    SELECT *
    FROM grade_filter
    WHERE CAST(revol_util AS FLOAT64) < 90
),

loan_summary AS (

SELECT

    customer_id,

    customer_name,

    city,

    state,

    customer_segment,

    occupation,

    employer,

    CAST(loan_amnt AS FLOAT64) AS loan_amnt,

    CAST(int_rate AS FLOAT64) AS int_rate,

    CAST(annual_inc AS FLOAT64) AS annual_inc,

    CAST(dti AS FLOAT64) AS dti,

    grade,

    CAST(revol_util AS FLOAT64) AS revol_util,

    CAST(out_prncp AS FLOAT64) AS out_prncp,

    issue_d,

    ROW_NUMBER() OVER (
        PARTITION BY customer_id
        ORDER BY CAST(loan_amnt AS FLOAT64) DESC
    ) AS rn,

    COUNT(*) OVER (
        PARTITION BY customer_id
    ) AS total_loans,

    SUM(CAST(loan_amnt AS FLOAT64)) OVER (
        PARTITION BY customer_id
    ) AS total_amount,

    AVG(CAST(int_rate AS FLOAT64)) OVER (
        PARTITION BY customer_id
    ) AS avg_interest

FROM utilization_filter

),

final_data AS (

SELECT *
FROM loan_summary

)

SELECT

    customer_name,

    city,

    state,

    occupation,

    employer,

    customer_segment,

    grade,

    loan_amnt,

    annual_inc,

    dti,

    revol_util,

    out_prncp,

    total_loans,

    total_amount,

    avg_interest,

    issue_d

FROM final_data

WHERE rn = 1

ORDER BY

    total_amount DESC,

    avg_interest DESC,

    annual_inc DESC,

    customer_name;