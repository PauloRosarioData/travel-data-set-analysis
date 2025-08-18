create or replace table amex_data_set.F001_forecast_vs_actual  as (

  with dataset as (
  SELECT
    transaction_date,
    product_type,
    sum(daily_transaction_value) as volume
  FROM `amex_data_set.T002_data_filters`
  where
    country_code = "UK"
    and transaction_date >= "2025-01-01"
  group by 1, 2
)

SELECT
  extract(month from transaction_date) as month,
  FORMAT_DATE('%B', transaction_date) as month_name,
  product_type,
  sum(volume) as actual
from dataset
group by
  1,2,3
ORDER BY
  1,3);