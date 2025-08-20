create or replace table amex_data_set.DF002_dataset_construction  as
(

WITH
  data_2025 AS (
  SELECT
    * except(product_type),
    DATE_TRUNC(transaction_date, MONTH) AS reference_month,
    EXTRACT(DAYOFWEEK
  FROM
    transaction_date) as day_of_the_week,
    upper(product_type) as product_type
  FROM
    `amex_data_set.T002_data_filters`
  WHERE
    country_code = "UK"
    AND transaction_date >= "2025-01-01"),

    season_factor as (SELECT * except(product_type), upper(product_type) as product_type from amex_data_set.DF001_seasonality_factor),

    adding_seasonality_factors as (


SELECT
  d.*, s.week_factor, f.Forecast as monthly_forecast
FROM
  data_2025 d
left join season_factor s
on d.product_type = s.product_type
and d.day_of_the_week = s.day_of_the_week

left join amex_data_set.F002_forecast_vs_actual f
on  d.product_type = f.PRODUCT
and d.reference_month = f.Period

order by transaction_date)

SELECT *, (week_factor/sum(week_factor) OVER (PARTITION BY product_type, reference_month))*monthly_forecast as daily_forecast from adding_seasonality_factors);