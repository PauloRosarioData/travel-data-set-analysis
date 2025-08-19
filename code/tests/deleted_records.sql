CREATE OR REPLACE TABLE
  amex_data_set.deleted_records AS (
  SELECT
    *
  FROM
    `amex_data_set.T001_data_clean`
  WHERE
    wrong_record = 1
    order by transaction_date desc);