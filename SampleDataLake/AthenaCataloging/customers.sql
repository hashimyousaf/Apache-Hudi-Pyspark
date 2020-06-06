CREATE EXTERNAL TABLE IF NOT EXISTS hash_demo.hudi_historical_customers (
  `customer_id` string,
  `first_name` string,
  `email` string,
  `time_zone` string,
  `am_pm` string,
  `size` string,
  `web_domain` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = '1'
) LOCATION 's3://air-product-training-datalake/hashim-yousaf/historical-data/HudiTarget/customers/'
TBLPROPERTIES ('has_encrypted_data'='false');
