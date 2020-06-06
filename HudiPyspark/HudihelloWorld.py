from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.types import *

# Command that I have been using to execute this script on EMR
# spark-submit --master yarn --deploy-mode cluster --jars commons-codec_commons-codec-1.11.jar,org.apache.hudi_hudi-spark-bundle_2.11-0.5.1-incubating.jar,commons-logging_commons-logging-1.2.jar,org.apache.httpcomponents_httpclient-4.5.9.jar,org.apache.httpcomponents_httpcore-4.4.11.jar,org.apache.spark_spark-avro_2.11-2.4.4.jar,org.spark-project.spark_unused-1.0.0.jar HudihelloWorld.py

table_name = "departments"
base_path = "s3://testhasbucket/"
source_path = base_path + "console/csv/departments.csv"
HudiTargetBucket = base_path + "HudiTarget/"
updated_source_path = base_path + "console/csv/upsated_departments.csv"
deleted_dataset = base_path + "console/csv/delete_departments.csv"

def get_spark_session(app_name='hudi_app'):
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

def inset_into_hudi(session):
    df = session.read.format("csv").option("header", "true")\
        .load(source_path)
    print("============= inset_into_hudi Department Data =============")
    df.show()
    print("=="*100)
    df.write.format("hudi") \
        .option("hoodie.upsert.shuffle.parallelism", 2) \
        .option("hoodie.insert.shuffle.parallelism", 2) \
        .option("hoodie.datasource.write.precombine.field", "dept_no") \
        .option("hoodie.datasource.write.recordkey.field", "dept_no") \
        .option("hoodie.table.name", table_name) \
        .mode("overwrite") \
        .save(HudiTargetBucket)

def query_filtered_inserted_data(session):
    df = session.read.format("hudi").load(HudiTargetBucket + "*")
    print("============= query_filtered_inserted_data Department Data =============")
    df.show()

    dept_view_name = "departmentsTempView"
    df.createOrReplaceTempView(dept_view_name)
    selected_df = session.sql("select * from {} where `dept_no` in ('d001', 'd002', 'd003')".format(dept_view_name))
    print("============= Selected Data =============")
    selected_df.show()

# Slow chaging dimension

def update_data(session):
    print("============= update_data Department Data =============")
    df = session.read.format("csv").option("header", "true") \
        .load(updated_source_path)
    df.show()
    print("==" * 100)
    df.write.format("hudi") \
        .option("hoodie.upsert.shuffle.parallelism", 2) \
        .option("hoodie.insert.shuffle.parallelism", 2) \
        .option("hoodie.datasource.write.precombine.field", "dept_no") \
        .option("hoodie.datasource.write.recordkey.field", "dept_no") \
        .option("hoodie.table.name", table_name) \
        .mode("append") \
        .save(HudiTargetBucket)
def delete_data(session):
    print("============= delete_data Department Data =============")
    df = session.read.format("csv").option("header", "true") \
        .load(deleted_dataset)
    df.show()
    print("==" * 100)
    df.write.format("hudi") \
        .option("hoodie.datasource.write.operation", "delete") \
        .option("hoodie.datasource.write.precombine.field", "dept_no") \
        .option("hoodie.datasource.write.recordkey.field", "dept_no") \
        .option("hoodie.table.name", table_name) \
        .mode("append") \
        .save(HudiTargetBucket)

def query_inserted_data(session):
    df = session.read.format("hudi").load(HudiTargetBucket + "*")
    print("============= query_inserted_data Department Data =============")
    df.show()

    dept_view_name = "query_inserted_data_view"
    df.createOrReplaceTempView(dept_view_name)
    selected_df = session.sql("select * from {} where 1=1".format(dept_view_name))
    print("============= Selected Data =============")
    selected_df.show()

def run_hudi_insert_operation(spark_session, source_path, write_operation, table_name, target_path ):

    df = spark_session.read.format("csv").option("header", "true") \
        .load(source_path)
    print("============= inset_into_hudi Department Data =============")
    df.show()
    print("==" * 100)
    df.write.format("hudi") \
        .option("hoodie.upsert.shuffle.parallelism", 2) \
        .option("hoodie.insert.shuffle.parallelism", 2) \
        .option("hoodie.datasource.write.precombine.field", "sales_id") \
        .option("hoodie.datasource.write.recordkey.field", "sales_id") \
        .option("hoodie.datasource.write.operation", write_operation) \
        .option("hoodie.table.name", table_name) \
        .mode("append") \
        .save(target_path)

def get_data(session, hudi_source_path):
    df = session.read.format("hudi").load(hudi_source_path + "*")
    print("============= query_inserted_data Department Data =============")
    df.show()

    dept_view_name = "query_inserted_data_view"
    df.createOrReplaceTempView(dept_view_name)
    selected_df = session.sql("select * from {} where 1=1".format(dept_view_name))
    print("============= Selected Data =============")
    selected_df.show()

if __name__ == '__main__':
    insert_sales_table_name = "insert-sales"
    bulk_insert_sales_table_name = "bulk_insert-sales"
    upsert_sales_table_name = "upsert-sales"
    delete_sales_table_name =  "upsert-sales"

    insert_sales_target_path = "s3://air-product-training-datalake/hashim-yousaf/hudi-poc/insert-sales/"
    bulk_insert_sales_target_path = "s3://air-product-training-datalake/hashim-yousaf/hudi-poc/bulk-insert-sales/"
    upsert_sales_target_path = "s3://air-product-training-datalake/hashim-yousaf/hudi-poc/upsert-sales/"
    delete_sales_target_path = "s3://air-product-training-datalake/hashim-yousaf/hudi-poc/delete-sales/"

    sales1_source_path = "s3://air-product-training-datalake/hashim-yousaf/hudi-poc/data/sales1.csv"
    sales2_source_path = "s3://air-product-training-datalake/hashim-yousaf/hudi-poc/data/sales2.csv"
    sales_updated_source_path = "s3://air-product-training-datalake/hashim-yousaf/hudi-poc/data/sales_updated.csv"

    spark_session = get_spark_session()
    # Available write operations
    # .option("hoodie.datasource.write.operation", "upsert") \
    # .option("hoodie.datasource.write.operation", "insert") \
    # .option("hoodie.datasource.write.operation", "bulkinsert") \
    # .option("hoodie.datasource.write.operation", "delete") \

    run_hudi_insert_operation(spark_session, sales1_source_path, "insert", insert_sales_table_name,insert_sales_target_path)
    get_data(spark_session,insert_sales_target_path)

    # inset_into_hudi(spark_session)
    # print("**" * 200)
    # query_filtered_inserted_data(spark_session)
    # print("**" * 200)
    # update_data(spark_session)
    # print("**" * 200)
    # query_inserted_data(spark_session)
    # print("**" * 200)
    # delete_data(spark_session)
    # print("**" * 200)
    # query_inserted_data(spark_session)
    # spark_session.stop()


