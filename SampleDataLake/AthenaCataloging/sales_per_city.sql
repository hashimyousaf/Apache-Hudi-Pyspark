CREATE EXTERNAL TABLE IF NOT EXISTS hash_demo.sales_per_city (
  `sales_id` string,
  `product_name` string,
  `city` string,
  `price` string,
  `quantity` string,
  `updated_at` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = '1'
) LOCATION 's3://air-product-training-datalake/hashim-yousaf/curated-data/sales_per_city/'
TBLPROPERTIES ('has_encrypted_data'='false');