CREATE OR REPLACE TABLE
  amex_data_set.F002_forecast_vs_actual AS (
  with forecast_data as (SELECT
  PARSE_DATE('%Y %B', CONCAT('2025 ', Month)) AS Period,
  COUNTRY,
  PRODUCT,
  TRANSACTION
FROM
  `amex_data_set.forecast` f
  Order by 1),

  actuals as (select * FROM amex_data_set.F001_forecast_vs_actual),

  all_dates as (
    SELECT Period as p from forecast_data
    group by 1
    UNION ALL
    SELECT month as p from amex_data_set.F001_forecast_vs_actual
    group by 1
  ),

  all_products as (
    SELECT PRODUCT from forecast_data
    group by 1
  ),

  producing_table as (

  SELECT p as Period, PRODUCT from all_dates cross join all_products
  group by 1,2
  order by 1,2)

  SELECT pt.*,a.total_volume/1000 as actual, a.actual_UK_only/1000 as actual_UK_only,
  a.UK_transactions as UK_transactions,a.total_transactions as total_transactions,
  f.TRANSACTION as Forecast,
  safe_divide(UK_transactions,f.TRANSACTION) as actual_vs_forecast
   from producing_table pt

  left join forecast_data f
  on pt.Period =  f.Period
  and pt.Product = f.PRODUCT

  left join actuals a
  on pt.Period =  a.month
  and pt.Product = a.product_type


  order by Period desc);