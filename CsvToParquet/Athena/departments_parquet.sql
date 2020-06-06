CREATE TABLE hash_demo.departments_parquet
WITH (
format = 'PARQUET',
parquet_compression = 'SNAPPY',
external_location = 's3://testhasbucket/programmatic/parquet'
      ) AS SELECT * FROM hash_demo.departments
