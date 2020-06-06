from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import boto3
import sys
import json

base_path = "s3://air-product-training-datalake/hashim-yousaf/historical-data/"
HudiTargetBucket = base_path + "HudiTarget/"
ssm_key_name_sales = "/ap/tables/hashim/sales"
ssm_key_name_customers = "/ap/tables/hashim/customers"
staging_bukcet = 'hashim-yousaf/staging-data/'

HOODIE_UPSERT_SHUFFLE_PARALLEISM = "hoodie.upsert.shuffle.parallelism"
HOODIE_INSERT_SHUFFLE_PARALLEISM = "hoodie.insert.shuffle.parallelism"
HOODIE_DATASOURCE_WRITE_PRECOMBINE_FIELD = "hoodie.datasource.write.precombine.field"
HOODIE_DATASOURCE_WRITE_RECORDKEY_FIELD = "hoodie.datasource.write.recordkey.field"
HOODIE_DATASOURCE_WRITE_PARTITIONPATH_FIELD = "hoodie.datasource.write.partitionpath.field"

ssm_key_name = sys.argv[1]
# timestamp = sys.argv[2]
list_of_buckets = sys.argv[2].replace('[', '').replace(']', '').replace("'", '').split(',')

def get_ssm_parameter(ssm_key_name):
    client = boto3.client('ssm')
    response = client.get_parameter( Name=ssm_key_name,WithDecryption=False)
    return eval(response['Parameter']['Value'])

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

def inset_into_hudi(session, source_path, hudi_target_bucket,table_name, mode, table_configs):
    df = session.read.format("parquet").load(source_path)
    print("============= inset_into_hudi Department Data =============")
    print("Source = {}, Target = {}, mode = {}".format(source_path, hudi_target_bucket, mode))
    df.show()
    print("=="*100)
    df.write.format("hudi") \
        .option("hoodie.upsert.shuffle.parallelism", table_configs[HOODIE_UPSERT_SHUFFLE_PARALLEISM]) \
        .option("hoodie.insert.shuffle.parallelism", table_configs[HOODIE_INSERT_SHUFFLE_PARALLEISM]) \
        .option("hoodie.datasource.write.precombine.field", table_configs[HOODIE_DATASOURCE_WRITE_PRECOMBINE_FIELD]) \
        .option("hoodie.datasource.write.recordkey.field", table_configs[HOODIE_DATASOURCE_WRITE_RECORDKEY_FIELD]) \
        .option("hoodie.datasource.write.partitionpath.field", table_configs[HOODIE_DATASOURCE_WRITE_PARTITIONPATH_FIELD]) \
        .option("hoodie.table.name", table_name) \
        .mode(mode) \
        .save(hudi_target_bucket)
    batch_recently_processed =  source_path.split("/")[5]
    update_ssm_value(table_configs, batch_recently_processed)

def update_ssm_value(table_configs, batch_recently_processed):
    client = boto3.client('ssm')
    # response = client.get_parameter(Name=ssm_key_name, WithDecryption=False)
    # value_of_ssm = eval(response['Parameter']['Value'])
    print("Table Configs before update\n {} \n\n".format(table_configs))
    table_configs["processed_batch"] = batch_recently_processed
    print("\nTable Configs After update\n {} \n\n".format(table_configs))

    response = client.put_parameter(
        Name=ssm_key_name,
        Description='Information about the last processed batch of every table.',
        Value=json.dumps(table_configs),
        Type='String',
        Overwrite=True
    )
    print(response)

def main():
    table_configs = get_ssm_parameter(ssm_key_name)
    last_processed_batch = table_configs['processed_batch']
    table_name = table_configs['table_name']
    if last_processed_batch == "-1":
        print("Prcessed Batch = {}".format(last_processed_batch))
        mode = "overwrite"
    else:
        print("Procssed Batch = {}".format(last_processed_batch))
        mode = "append"

    spark_session = get_spark_session()
    stripped_list_of_bucket = [x.strip() for x in list_of_buckets]
    target_bucket = HudiTargetBucket + table_name + "/"
    print("Table name =  {}".format(table_name))
    print("Last Procssed batch time  =  {}".format(last_processed_batch))
    print("Target Bucket Path = {}".format(target_bucket))
    print("Table configs = {}".format(table_configs))
    list_index=0
    for source_bucket  in stripped_list_of_bucket:
        print("\n\n\nHere are the source bucket : {}\n\n\n".format(source_bucket))
        if list_index == 1:
            mode="append"
        inset_into_hudi(spark_session, source_bucket, target_bucket, table_name, mode, table_configs)
        list_index += 1
        print("\n\n\nProcessing completed for path : {}\n\n\n".format(source_bucket))

if __name__ == '__main__':
    main()



