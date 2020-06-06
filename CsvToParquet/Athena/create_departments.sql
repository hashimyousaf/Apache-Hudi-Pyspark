CREATE EXTERNAL TABLE IF NOT EXISTS hash_demo.departments (
  `dept_no` string,
  `dept_name` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = ',',
  'field.delim' = ','
) LOCATION 's3://testhasbucket/programmatic/csv/'
TBLPROPERTIES ('has_encrypted_data'='false');
