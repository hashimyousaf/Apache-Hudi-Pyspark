CREATE EXTERNAL TABLE IF NOT EXISTS hash_demo.hudi_historical_sales (
  `sales_id` string,
  `customer_id` string,
  `product_name` string,
  `price` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = '1'
) LOCATION 's3://air-product-training-datalake/hashim-yousaf/historical-data/HudiTarget/sales/'
TBLPROPERTIES ('has_encrypted_data'='false');
