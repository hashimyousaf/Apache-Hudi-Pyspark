from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import boto3
import sys
import json

sales_hudi_path = 's3://air-product-training-datalake/hashim-yousaf/historical-data/HudiTarget/sales/'
customer_hudi_path = 's3://air-product-training-datalake/hashim-yousaf/historical-data/HudiTarget/customers/'
HudiTargetBucketsToCheckCount = [sales_hudi_path, customer_hudi_path]

def get_spark_session(app_name='hudi_count_check_app'):
    hudi_jars = 's3://air-product-training-datalake/hashim-yousaf/jars/commons-codec_commons-codec-1.11.jar,' \
                's3://air-product-training-datalake/hashim-yousaf/jars/org.apache.hudi_hudi-spark-bundle_2.11-0.5.1-incubating.jar,' \
                's3://air-product-training-datalake/hashim-yousaf/jars/commons-logging_commons-logging-1.2.jar,' \
                's3://air-product-training-datalake/hashim-yousaf/jars/org.apache.httpcomponents_httpclient-4.5.9.jar,' \
                's3://air-product-training-datalake/hashim-yousaf/jars/org.apache.httpcomponents_httpcore-4.4.11.jar,' \
                's3://air-product-training-datalake/hashim-yousaf/jars/org.apache.spark_spark-avro_2.11-2.4.4.jar,' \
                's3://air-product-training-datalake/hashim-yousaf/jars/org.spark-project.spark_unused-1.0.0.jar'
    conf = SparkConf() \
        .set('spark.serializer', 'org.apache.spark.serializer.KryoSerializer') \
        .set('spark.sql.hive.convertMetastoreParquet', 'false') \
        .set('spark.jars', hudi_jars) \
        .set('spark.port.maxRetries', '40')\
        .set("spark.executor.instances", "4")\
        .set("spark.executor.cores", "2")
    return SparkSession.builder.appName(app_name) \
        .config(conf=conf).enableHiveSupport().getOrCreate()

def query_inserted_data(session):

    for hudi_path in HudiTargetBucketsToCheckCount:
        df = session.read.format("hudi").load(hudi_path + "*")
        print("============= query_inserted_data Department Data =============")
        # df.show()

        dept_view_name = "query_inserted_data_view"
        df.createOrReplaceTempView(dept_view_name)
        selected_df = session.sql("select count(*) from {} where 1=1".format(dept_view_name))
        print("============= Selected Data =============")
        selected_df.show()
        print("\nAbove is the result of path =={}\n\n".format(hudi_path))
if __name__ == '__main__':
    spark_session = get_spark_session()
    query_inserted_data(spark_session)
    exit(0)