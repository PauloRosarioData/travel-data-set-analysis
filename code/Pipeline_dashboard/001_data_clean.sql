create or replace table amex_data_set.T001_data_clean as ( with data_load as (SELECT
  cast(TRANSACTION_DATE as date) as TRANSACTION_DATE,
  cast(country as string) as country,
  cast(product as string) as product,
  cast(TRANSACTION_CNT as numeric) as TRANSACTION_CNT,
  cast(VALUE as numeric) as VALUE
FROM
  `amex_data_set.clienttrends`)

  SELECT * from data_load
  ---see script test where we identify some negative transactions
  ---presumably either refunds, or some data quality issue
  ---for the sake of this analysis, removing it
  where transaction_cnt > 0 and 
  ----in scrip yyy we also identify some transactions with negative values
  ---- for the sake of this analysis will remove it
  ---but traditionally would confirm the data source and the actual meaning of it
  value > 0 
  ---in script yyy we have identified some outliers
  --- we would check if they are real or not (actual client transaction or data quality issue)
  ---for the sake of this analysis I'm considering it a data quality issue and removing it
  and value/transaction_cnt < 50000);
