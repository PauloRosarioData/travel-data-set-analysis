create or replace table amex_data_set.T001_data_clean as (
   with data_load as (SELECT
   *,
  cast(TRANSACTION_DATE as date) as tr_date,
  cast(country as string) as country_name,
  cast(product as string) as product_type,
  safe_cast(TRANSACTION_CNT as DECIMAL) as transaction_count,
  ---because of the excel formating of the source file
  ---there might be a comma used as a thousand mark
  --we also have 0 or missing values coded as "-"
  CAST(REPLACE(REPLACE(VALUE, '-', '0'), ',', '') AS DECIMAL) as transaction_value
FROM
  `amex_data_set.clienttrends`),

  flagging_errors_in_dataset as (

  SELECT *,
  ---our test xx identified that the column country is mispecified
  --having different names for the same country, fixing this
  ---with the variable country_code
  CASE 
  WHEN country_name in ("FR", "France") then "FR"
  WHEN country_name in ("IT", "Italy") then "IT"
  WHEN country_name in ("United Kingdom") then "UK"
  WHEN country_name in ("United States of America") then "US"
  ELSE "not specified" end as country_code,
  ---we have identified 113 records with 0 transactions,
  ---or negative values of transactions
  ---would traditionally investigate the origin of this error/data issue
  ----but for simplicity and given the limited number will create a flag for those
  ---records and will eliminate for our analysis
  if(transaction_count >0,0,1) as wrong_transaction_cnt,
  ---similarly there are several records with negative values 
  ---for the column value of transaction
  ---if a real task would investigate the source of this data error
  ---but for now the same method, flag and ignore
  if(transaction_value >0,0,1) as wrong_transaction_value,
  --we have also identified a specific record
  ---(27/1/2024 air italy) with a value of over 1 million for 3 transactions
  ---magnitudes higher than any record
  ---again in a tradional analyis woul find the source of that data point
  ---but for now assuming its an error so flagging 
  ---as discussed we have some incidences of transaction_cnt = 0 
  ---so we use safe_divide
  if(safe_divide(transaction_value,transaction_count) > 20000,1,0) as wrong_outlier_value

  from data_load)

  SELECT *, 
  ---if a record is captured by any of the three previous check will get a flag for deletion
  GREATEST(wrong_transaction_cnt, wrong_transaction_value, wrong_outlier_value) AS wrong_record
  from flagging_errors_in_dataset
  );
