import sys
from pyspark.sql import SparkSession
from pyspark import SparkConf
from datetime import datetime
from pyspark.sql.functions import lit

# This script will take batch_id and read from staging buckets and put it into spark_historical
batch_id = sys.argv[1]
# This script requires batch_id from staging buckets.
# Also make sure you have relevant buckets in position.



historical_path =  "s3://air-product-training-datalake/hashim-yousaf/spark-historical/"
staging_path = "s3://air-product-training-datalake/hashim-yousaf/staging-data/" + batch_id + "/"
table_dict = {'sales':'product_name', 'customers':'city'}

def get_spark_session(app_name='Historical_App'):
    conf = SparkConf() \
        .set('spark.serializer', 'org.apache.spark.serializer.KryoSerializer') \
        .set('spark.sql.hive.convertMetastoreParquet', 'false') \
        .set('spark.port.maxRetries', '40')\
        .set("spark.executor.instances", "4")\
        .set("spark.executor.cores", "2")
    return SparkSession.builder.appName(app_name) \
        .config(conf=conf).enableHiveSupport().getOrCreate()

def get_and_put_data(session, table):
    df = session.read.format("parquet").option("mergeSchema", "true").\
        load(staging_path + table)
    ts = int(datetime.timestamp(datetime.now()))
    print("Timestamp taken is", ts)
    df = df.withColumn("timestamp", lit(ts))
    df.write.mode('append').partitionBy(table_dict[table])\
        .parquet("{}{}/".format(historical_path, table))


def main():
    spark_session = get_spark_session()
    print("Starting historical data insertion")
    for table  in table_dict:
        df = get_and_put_data(spark_session, table)

if __name__ == '__main__':
    main()