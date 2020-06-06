from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import json
import  sys
import boto3
from pyspark.sql.functions import lit


SALES = 'sales'
CUSTOMERS = 'customers'
landing_bucket_name = 'landing-data'
staging_bucket_name = 'staging-data'
base_bucket = 's3://air-product-training-datalake/'
ssm_key_name = '/ap/tables/kmskey'


table_name = sys.argv[1]
timestamp = sys.argv[2]
list_of_buckets = sys.argv[3].replace('[', '').replace(']', '').replace("'", '').split(',')


def convertCSVToParquet():
    conf = SparkConf().setMaster("local[2]").setAppName("CSV2Parquet")
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    print("****" * 10)
    print("Below are the parameters passed to the file.")
    print(table_name)
    print(timestamp)
    print(list_of_buckets)
    print("****" * 10)
    stripped_list_of_bucket = [x.strip() for x in list_of_buckets]
    print("List of bucket after Stripping \n {}".format(stripped_list_of_bucket))

    for source_bucket in stripped_list_of_bucket:
        df = sqlContext.read.format("csv").option("header", "true") \
            .load(base_bucket + source_bucket)

        target_bucket = source_bucket.replace(landing_bucket_name, staging_bucket_name).split('/')
        target_bucket[-1] = ""  # target_bucket[-1].replace('.csv', '.parquet')
        last_processed_batch = target_bucket[2]
        target_bucket_path = '/'.join([str(elem) for elem in target_bucket])
        print("Converted Landed Path :: {} \n to its staging Path :: {}"
              .format(source_bucket, target_bucket_path))

        target_file_path = base_bucket + target_bucket_path
        print("Target file path becomes :: {}".format(target_file_path))

        if table_name.lower() == CUSTOMERS:
            print("Schema Selectes = {}".format(SALES))
            partition_column = "city"
        elif table_name.lower() == SALES:
            print("Schema Selectes = {}".format(CUSTOMERS))
            partition_column = "product_name"
        else:
            pass
        df = df.withColumn("batch_id", lit(last_processed_batch))
        df.write.mode('overwrite').partitionBy(partition_column).parquet(target_file_path)
        update_ssm_key_for_the_table(table_name, last_processed_batch)

# Code Reference : http://blogs.quovantis.com/how-to-convert-csv-to-parquet-files/

def update_ssm_key_for_the_table(table_name, timestamp):
    client = boto3.client('ssm')
    response = client.get_parameter(Name=ssm_key_name, WithDecryption=False)
    value_of_ssm = eval(response['Parameter']['Value'])
    print(value_of_ssm)
    value_of_ssm[table_name] = timestamp

    print("Updated SSM Params \n {}".format(value_of_ssm))

    response = client.put_parameter(
        Name=ssm_key_name,
        Description='Information about the last processed batch of every table.',
        Value=json.dumps(value_of_ssm),
        Type='String',
        Overwrite=True
    )
    print(response)


if __name__ == "__main__":
    convertCSVToParquet()
    # update_ssm_key_for_the_table('customers', '-1')
    exit(0)
