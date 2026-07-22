-- resources/sql/delinquency_alerts.sql
--comment 1
--comment 2
SELECT *
FROM `project-ff7c2ef5-8d88-401a-b86.loan_data.loan` l
JOIN `project-ff7c2ef5-8d88-401a-b86.loan_data.customers` c
  ON l.customer_id = c.customer_id
WHERE CAST(l.delinq_2yrs AS INT64) > 0
   OR l.mths_since_last_delinq IS NOT NULL