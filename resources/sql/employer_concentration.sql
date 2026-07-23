-- resources/sql/employer_concentration.sql
WITH loan_data AS (
    SELECT *
    FROM `project-ff7c2ef5-8d88-401a-b86.loan_data.loan`
),
customer_data AS (
    SELECT *
    FROM `project-ff7c2ef5-8d88-401a-b86.loan_data.customers`
)
SELECT
    c.employer,
    l.loan_amnt,
    l.emp_length
FROM loan_data l
JOIN customer_data c ON l.customer_id = c.customer_id
WHERE c.employer IS NOT NULL