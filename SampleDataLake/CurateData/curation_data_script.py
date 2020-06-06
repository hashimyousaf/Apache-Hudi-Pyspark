from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import boto3
import sys
import json

is_first_load = sys.argv[1]
batch_id  = sys.argv[2]

base_path = "s3://air-product-training-datalake/hashim-yousaf/staging-data/" + batch_id + "/"
customers_path = base_path  + "customers"
sales_path = base_path + "sales"
target_hudi_path = "s3://air-product-training-datalake/hashim-yousaf/curated-data/"
source__path_historical_customers = "s3://air-product-training-datalake/hashim-yousaf/historical-data/HudiTarget/customers/"

HOODIE_UPSERT_SHUFFLE_PARALLEISM = "hoodie.upsert.shuffle.parallelism"
HOODIE_INSERT_SHUFFLE_PARALLEISM = "hoodie.insert.shuffle.parallelism"
HOODIE_DATASOURCE_WRITE_PRECOMBINE_FIELD = "hoodie.datasource.write.precombine.field"
HOODIE_DATASOURCE_WRITE_RECORDKEY_FIELD = "hoodie.datasource.write.recordkey.field"
HOODIE_DATASOURCE_WRITE_PARTITIONPATH_FIELD = "hoodie.datasource.write.partitionpath.field"

def get_ssm_parameter(ssm_key_name):
    print("Getting SSM key :: {}".format(ssm_key_name))
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

def get_sales_and_customers_df(spark_session):

    df_sales = spark_session.read.format("parquet").option("mergeSchema", "true").load(sales_path)
    print("Sales Path ::", sales_path)
    print("Sales COunt :: ", df_sales.count())
    df_sales.show(5)

    df_customers = spark_session.read.format("parquet").option("mergeSchema", "true").load(customers_path)
    print("Customers Path ::", customers_path)
    print("Customers COunt :: ",df_customers.count())
    df_customers.show(5)
    print("Sales and Customers data fetched for the batch {}".format(batch_id))
    return df_sales, df_customers

def resolve_dependency_problems(session, df_sales, df_customers):
    # This function will find dependency problem and returns customer after fetching it
    # from historical data.
    print("resolve_dependency_problems")
    # dependency_joined_df = df_sales.join(df_customers, df_sales.customer_id == df_customers.customer_id, 'left')
    dependency_joined_df = df_sales.select(df_sales.customer_id).subtract(df_customers.select(df_customers.customer_id))
    count = dependency_joined_df.count()
    print("Left join count result :: {}".format(count))
    if count > 0:
        # cust_customer_id = dependency_joined_df
        list_cust = [str(row.customer_id) for row in dependency_joined_df.collect()]
        where_clause_ids = ",".join(list_cust)
        where_clause_ids = "({})".format(where_clause_ids)
        fetch_customers_df = get_dependent_customers_from_historical(session, where_clause_ids)
        customers_after_union_df = df_customers.unionAll(fetch_customers_df)
        print("Dependency has been resolved till here.")
        # sales_per_city, sales_per_customers = get_sales_per_city_and_customers(df_sales, customers_after_union_df)
        return df_sales, customers_after_union_df
    else:
        # sales_per_city, sales_per_customers = get_sales_per_city_and_customers(df_sales, df_customers)
        return df_sales, df_customers

def get_dependent_customers_from_historical(session, where_clause_ids):
    df = session.read.format("hudi").load(source__path_historical_customers + "*")
    print("get_dependent_customers_from_historical data fetched")

    customers_view_name = "historical_customers_view"
    df.createOrReplaceTempView(customers_view_name)
    query = "select customer_id, first_name, email, time_zone, city, web_domain," \
            " updated_at, is_deleted, batch_id from {} where customer_id in {}"\
        .format(customers_view_name, where_clause_ids)

    print("Query becomes :: {}".format(query))
    selected_df = session.sql(query)
    count_value = selected_df.count()

    print("Query count result :: {}".format(count_value))
    if count_value > 0 :
        print("Count of historical fetched data = {}".format(count_value))
        return selected_df
    else:
        raise Exception("Dependency could not be resolved successfully.")

def get_sales_per_city_and_customers(df_sales, df_customers):
    df_joined = df_sales.join(df_customers, df_sales.customer_id == df_customers.customer_id, 'inner')

    sales_per_city = df_joined.select(df_sales.sales_id, df_sales.product_name, df_customers.city,
                                      df_sales.price, df_sales.quantity, df_sales.updated_at)
    print("sales_per_city count :: ", sales_per_city.count())
    sales_per_city.show(5)
    sales_per_customers = df_joined.select(df_sales.sales_id, df_sales.customer_id,
                                           df_customers.first_name, df_sales.price, df_sales.updated_at)
    print("sales_per_customers count :: ", sales_per_customers.count())
    sales_per_customers.show(5)
    return  sales_per_city, sales_per_customers

def put_curated_data_in_bucket(table_df, table_name, target_hudi_path ):
    table_configs = get_ssm_parameter("/ap/curation/{}".format(table_name))
    target_hudi_path = target_hudi_path + table_name + "/"
    if table_name == "sales_per_customers":
        print("Inserting sales_per_customers")
        table_df.write.format("hudi") \
            .option("hoodie.upsert.shuffle.parallelism", table_configs[HOODIE_UPSERT_SHUFFLE_PARALLEISM]) \
            .option("hoodie.insert.shuffle.parallelism", table_configs[HOODIE_INSERT_SHUFFLE_PARALLEISM]) \
            .option("hoodie.datasource.write.precombine.field", table_configs[HOODIE_DATASOURCE_WRITE_PRECOMBINE_FIELD]) \
            .option("hoodie.datasource.write.recordkey.field", table_configs[HOODIE_DATASOURCE_WRITE_RECORDKEY_FIELD]) \
            .option("hoodie.table.name", table_name) \
            .mode("append") \
            .save(target_hudi_path)
    else:
        print("Inserting Some other table in Curated bucket")
        table_df.write.format("hudi") \
            .option("hoodie.upsert.shuffle.parallelism", table_configs[HOODIE_UPSERT_SHUFFLE_PARALLEISM]) \
            .option("hoodie.insert.shuffle.parallelism", table_configs[HOODIE_INSERT_SHUFFLE_PARALLEISM]) \
            .option("hoodie.datasource.write.precombine.field", table_configs[HOODIE_DATASOURCE_WRITE_PRECOMBINE_FIELD]) \
            .option("hoodie.datasource.write.recordkey.field", table_configs[HOODIE_DATASOURCE_WRITE_RECORDKEY_FIELD]) \
            .option("hoodie.datasource.write.partitionpath.field",
                    table_configs[HOODIE_DATASOURCE_WRITE_PARTITIONPATH_FIELD]) \
            .option("hoodie.table.name", table_name) \
            .mode("append") \
            .save(target_hudi_path)

def delete_data(df,  table_name, hudi_target_path ):
    print("============= delete_data {} Data =============".format(table_name))
    target_hudi_path = hudi_target_path + table_name + "/"
    df.show(2)
    print("==" * 100)
    df.write.format("hudi") \
        .option("hoodie.datasource.write.operation", "delete") \
        .option("hoodie.datasource.write.precombine.field", "sale_id") \
        .option("hoodie.datasource.write.recordkey.field", "updated_at") \
        .option("hoodie.table.name", table_name) \
        .mode("append") \
        .save(target_hudi_path)

def process_deletion(sales_df, customers_df):
    sales_df =  sales_df.filter(sales_df.is_deleted == 0)
    customers_df = customers_df.filter(customers_df.is_deleted == 0)

    sales_to_delete =  sales_df.filter(sales_df.is_deleted == 1)
    customers_to_delete = customers_df.filter(customers_df.is_deleted == 1)
    delete_data(sales_to_delete, "sales_per_city", target_hudi_path)
    delete_data(sales_to_delete, "sales_per_customers", target_hudi_path)

    return sales_path, customers_df

def main():
    spark_session = get_spark_session()
    print("Spark session created")
    sales_df, customers_df = get_sales_and_customers_df(spark_session)
    if is_first_load != "true":
        sales_df, customers_df = resolve_dependency_problems(spark_session, sales_df, customers_df)
    sales_df, customers_df = process_deletion(spark_session, sales_df, customers_df)
    sales_per_city, sales_per_customers = get_sales_per_city_and_customers(sales_df, customers_df)
    put_curated_data_in_bucket(sales_per_city, "sales_per_city", target_hudi_path)
    put_curated_data_in_bucket(sales_per_customers, "sales_per_customers", target_hudi_path)

if __name__ == '__main__':
    main()

# get sales and customer data from staging batch
# see if there is any dependency problem there but if it's not the first batch
# if there is not any dependency then put it in curated table
# else fetch customer_id from sales which are not in customers table
# and query it in historical customer table
# and now union this dataset of customers with actual customers
# and join it and make curated table.
