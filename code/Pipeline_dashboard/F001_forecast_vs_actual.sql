create or replace table amex_data_set.F001_forecast_vs_actual  as (

  with dataset as (
  SELECT
    transaction_date,
    upper(product_type) as product_type,
    sum(if(country_code="UK",daily_transaction_value,0)) as UK_volume,
    sum(daily_transaction_value) as total_volume,
    sum(if(country_code="UK",daily_transaction_count,0)) as UK_transactions,
    sum(daily_transaction_count) as total_transactions
  FROM `amex_data_set.T002_data_filters`
  group by 1, 2
)

SELECT
  DATE_TRUNC(transaction_date, MONTH) AS month,
  product_type,
  sum(total_volume) as total_volume,
  sum(UK_volume) as actual_UK_only,
  sum(UK_transactions) as UK_transactions,
  sum(total_transactions) as total_transactions,

from dataset
group by
  1,2
ORDER BY
  1);