CREATE OR REPLACE TABLE amex_data_set.T003_data_agg_per_country AS
(

with agg_dataset as ( SELECT
  transaction_date,
  country_code,
  SUM(m28day_total_transaction_count) AS m28day_total_transaction_count,
  SUM(m28day_total_transaction_value) AS m28day_total_transaction_value
FROM
  `amex_data_set.T002_data_filters`
GROUP BY
  1,
  2)

  SELECT *,
  safe_divide(m28day_total_transaction_value,lag(m28day_total_transaction_value, 365) over (partition by country_code order by transaction_date))-1 as m28day_total_transaction_value_YoY,
    safe_divide(m28day_total_transaction_count,lag(m28day_total_transaction_count, 365) over (partition by country_code order by transaction_date))-1 as m28day_total_transaction_count_YoY
     from agg_dataset);