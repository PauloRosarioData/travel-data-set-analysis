create or replace table amex_data_set.Test_program as (
---a quick visual inspection of the file shows that it contains 5 columns
---

WITH dataset_to_test AS (
  SELECT 
    TRANSACTION_DATE, 
    COUNTRY, 
    PRODUCT, 
    TRANSACTION_CNT, 
    VALUE
  FROM `amex_data_set.clienttrends`
),

record_count_test AS (
  SELECT 
    "T01: number of records" AS test, 
    -- FIX: Use CAST to convert the count (an integer) to a STRING
    CAST(COUNT(*) AS STRING) AS test_value 
  FROM dataset_to_test
),

first_date AS (
  SELECT 
    "T02:first date" AS test, 
    -- FIX: Use CAST to convert the date to a STRING
    CAST(MIN(TRANSACTION_DATE) AS STRING) AS test_value 
  FROM dataset_to_test
),

last_date AS (
  SELECT 
    "T03:last date" AS test, 
    -- FIX: Use CAST to convert the date to a STRING
    CAST(max(TRANSACTION_DATE) AS STRING) AS test_value 
  FROM dataset_to_test
),

expected_number_of_dates AS (
  SELECT 
    "T04:expected number of dates" AS test, 
    -- FIX: Use CAST to convert the date to a STRING
    CAST(DATE_DIFF(MAX(TRANSACTION_DATE), MIN(TRANSACTION_DATE), DAY) + 1 AS STRING) AS test_value
  FROM dataset_to_test
),

actual_number_dates AS (
  SELECT 
    "T05:actual distinct dates" AS test, 
    -- FIX: Use CAST to convert the date to a STRING
    cast(count(distinct TRANSACTION_DATE) as string) AS test_value
  FROM dataset_to_test
),

merging_tests as (


SELECT * FROM record_count_test
UNION ALL
SELECT * FROM first_date
UNION ALL
SELECT * FROM last_date
UNION ALL
SELECT * FROM expected_number_of_dates
UNION ALL 
SELECT * from actual_number_dates

)

SELECT * from merging_tests
order by test asc);