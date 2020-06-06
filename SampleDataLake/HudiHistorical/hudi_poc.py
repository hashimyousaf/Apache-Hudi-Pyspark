from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession


insert_sales_table_name = "insert"
bulk_insert_sales_table_name = "bulkinsert"
upsert_sales_table_name = "upsert"
delete_sales_table_name = "delete"

insert_sales_target_path = "s3://air-product-training-datalake/hashim-yousaf/hudi-poc/insertsales/"
bulk_insert_sales_target_path = "s3://air-product-training-datalake/hashim-yousaf/hudi-poc/bulkinsertsales/"
upsert_sales_target_path = "s3://air-product-training-datalake/hashim-yousaf/hudi-poc/upsertsales/"
delete_sales_target_path = "s3://air-product-training-datalake/hashim-yousaf/hudi-poc/deletesales/"

sales1_source_path = "s3://air-product-training-datalake/hashim-yousaf/hudi-poc/data/sales1.csv"
sales2_source_path = "s3://air-product-training-datalake/hashim-yousaf/hudi-poc/data/sales2.csv"
sales_updated_source_path = "s3://air-product-training-datalake/hashim-yousaf/hudi-poc/data/sales_updated.csv"

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

def run_hudi_insert_operation(spark_session, source_path, write_operation, table_name, target_path):
    df = spark_session.read.format("csv").option("header", "true") \
        .load(source_path)
    print("============= inset_into_hudi Sales Data =============")
    df.show()
    print("==" * 100)
    df.write.format("hudi") \
        .option("hoodie.upsert.shuffle.parallelism", 2) \
        .option("hoodie.insert.shuffle.parallelism", 2) \
        .option("hoodie.datasource.write.precombine.field", "updated_at") \
        .option("hoodie.datasource.write.recordkey.field", "sales_id") \
        .option("hoodie.datasource.write.operation", write_operation) \
        .option("hoodie.table.name", table_name) \
        .mode("append") \
        .save(target_path)


# .option("hoodie.datasource.write.operation", write_operation) \


def get_data(session, hudi_source_path):
    df = session.read.format("hudi").load(hudi_source_path + "*")
    print("============= query_inserted_data Department Data =============")
    df.show()

    # dept_view_name = "query_inserted_data_view"
    # df.createOrReplaceTempView(dept_view_name)
    # selected_df = session.sql("select * from {} where 1=1".format(dept_view_name))
    # print("============= Selected Data =============")
    # selected_df.show()


if __name__ == '__main__':

    spark_session = get_spark_session()
    # Available write operations
    # .option("hoodie.datasource.write.operation", "upsert") \
    # .option("hoodie.datasource.write.operation", "insert") \
    # .option("hoodie.datasource.write.operation", "bulkinsert") \
    # .option("hoodie.datasource.write.operation", "delete") \

    write_mode = "insert"
    run_hudi_insert_operation(spark_session, sales2_source_path, write_mode, insert_sales_table_name, insert_sales_target_path)

    print(sales2_source_path)
    print(write_mode)
    print(insert_sales_table_name + "_table")
    print(insert_sales_target_path)

    print("\n\n\nhere we are getting data back\n\n\n")
    get_data(spark_session, insert_sales_target_path)
