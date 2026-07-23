-- resources/sql/delinquency_alerts.sql
--dummy 1
SELECT
    l.customer_id,
    c.customer_name,
    l.delinq_2yrs,
    l.mths_since_last_delinq,
    l.loan_status,
    l.term
FROM `project-ff7c2ef5-8d88-401a-b86.loan_data.loan` l
JOIN `project-ff7c2ef5-8d88-401a-b86.loan_data.customers` c
  ON l.customer_id = c.customer_id
WHERE CAST(l.delinq_2yrs AS INT64) > 0
   OR l.mths_since_last_delinq IS NOT NULL