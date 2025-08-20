create or replace table amex_data_set.DF001_seasonality_factor  as
(

SELECT
  EXTRACT(DAYOFWEEK
  FROM
    transaction_date) as day_of_the_week,
  product_type,
  AVG(daily_transaction_count) as week_factor
FROM
  `amex_data_set.T002_data_filters`
where country_code = "UK" and transaction_date < "2025-01-01"
GROUP BY
  1,
  2);