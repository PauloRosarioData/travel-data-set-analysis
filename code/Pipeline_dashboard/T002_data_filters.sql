CREATE OR REPLACE TABLE amex_data_set.T002_data_filters AS
(
  WITH base_data AS (
    SELECT
      tr_date AS transaction_date,
      country_code,
      product_type,
      SUM(transaction_count) AS transaction_count,
      SUM(transaction_value) AS transaction_value
    FROM
      amex_data_set.T001_data_clean
    WHERE
      wrong_record = 0
    GROUP BY
      1, 2, 3
  ),
  -- Step 1: Get unique combinations of country_code and product_type
  -- and determine the overall min/max transaction dates from the base data
  date_and_dimensions AS (
    SELECT
      MIN(transaction_date) AS min_date,
      MAX(transaction_date) AS max_date,
      country_code,
      product_type
    FROM
      base_data
    GROUP BY
      country_code,
      product_type
  ),
  -- Step 2: Generate all possible dates between min and max transaction date
  -- and cross join with all country_code and product_type combinations
  all_possible_combinations AS (
    SELECT
      generated_date AS transaction_date,
      dd.country_code,
      dd.product_type
    FROM
      date_and_dimensions AS dd,
      UNNEST(GENERATE_DATE_ARRAY(
        (SELECT MIN(min_date) FROM date_and_dimensions),
        (SELECT MAX(max_date) FROM date_and_dimensions),
        INTERVAL 1 DAY
      )) AS generated_date
    GROUP BY
      generated_date,
      dd.country_code,
      dd.product_type
  )
  -- Step 3: LEFT JOIN the generated combinations with the actual base_data
  -- to ensure all dates for all dimensions are present, filling missing transaction values with 0
  SELECT
    apc.transaction_date,
    apc.country_code,
    apc.product_type,
    COALESCE(bd.transaction_count, 0) AS daily_transaction_count,
    COALESCE(bd.transaction_value, 0) AS daily_transaction_value,
    -- Step 4: Calculate 7-day rolling totals for transaction_count
    SUM(COALESCE(bd.transaction_count, 0)) OVER (
      PARTITION BY apc.country_code, apc.product_type
      ORDER BY apc.transaction_date ASC
      ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS seven_day_total_transaction_count,
    -- Step 5: Calculate 7-day rolling totals for transaction_value
    SUM(COALESCE(bd.transaction_value, 0)) OVER (
      PARTITION BY apc.country_code, apc.product_type
      ORDER BY apc.transaction_date ASC
      ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS seven_day_total_transaction_value,
    -- Step 5: Calculate 28-day rolling totals for transaction_count
    SUM(COALESCE(bd.transaction_count, 0)) OVER (
      PARTITION BY apc.country_code, apc.product_type
      ORDER BY apc.transaction_date ASC
      ROWS BETWEEN 27 PRECEDING AND CURRENT ROW
    ) AS m28day_total_transaction_count,
    -- Step 5: Calculate 7-day rolling totals for transaction_value
    SUM(COALESCE(bd.transaction_value, 0)) OVER (
      PARTITION BY apc.country_code, apc.product_type
      ORDER BY apc.transaction_date ASC
      ROWS BETWEEN 27 PRECEDING AND CURRENT ROW
    ) AS m28day_total_transaction_value
  FROM
    all_possible_combinations AS apc
  LEFT JOIN
    base_data AS bd
    ON apc.transaction_date = bd.transaction_date
    AND apc.country_code = bd.country_code
    AND apc.product_type = bd.product_type
  ORDER BY
    apc.country_code,
    apc.product_type,
    apc.transaction_date
);