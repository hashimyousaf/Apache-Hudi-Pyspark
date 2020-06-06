from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import boto3
import sys
from pyspark.sql.functions import col
import json

# is_first_load = sys.argv[1]
# batch_id  = sys.argv[2]

#
# base_path = "s3://air-product-training-datalake/hashim-yousaf/staging-data/" + batch_id + "/"
# customers_path = base_path  + "customers"
# sales_path = base_path + "sales"
# target_sales_per_city_curation_path = "s3://air-product-training-datalake/hashim-yousaf/spark-curation/sales_per_city/"
# target_sales_per_customer_curation_path = "s3://air-product-training-datalake/hashim-yousaf/spark-curation/sales_per_customers/"
# source_path_historical_customers = "s3://air-product-training-datalake/hashim-yousaf/spark-historical/customers/"

#=================Batch1=================
# is_first_load = "true"
# batch_id  = "assumed_batch"
#
# customers_path = "C:\\Users\hashim.yousaf\Desktop\JupyterNotebooks\HudiOperations\Batch1\customers.csv"
# sales_path = "C:\\Users\hashim.yousaf\Desktop\JupyterNotebooks\HudiOperations\Batch1\sales.csv"
#
# target_sales_per_city_curation_path = "C:\\Users\hashim.yousaf\Desktop\JupyterNotebooks\HudiOperations\Curated\sales_per_city\\"
# target_sales_per_customer_curation_path = "C:\\Users\hashim.yousaf\Desktop\JupyterNotebooks\HudiOperations\Curated\sales_per_customers\\"
# source_path_historical_customers = "C:\\Users\hashim.yousaf\Desktop\JupyterNotebooks\HudiOperations\Historical\customers\\"
#=================Batch1=================

#=================Batch2=================
is_first_load = "true"
batch_id  = "assumed_batch"
customers_path = "C:\\Users\hashim.yousaf\Desktop\JupyterNotebooks\HudiOperations\Batch2\customers2.csv"
sales_path = "C:\\Users\hashim.yousaf\Desktop\JupyterNotebooks\HudiOperations\Batch2\sales2.csv"

target_sales_per_city_curation_path = "C:\\Users\hashim.yousaf\Desktop\JupyterNotebooks\HudiOperations\Curated\sales_per_city\\"
target_sales_per_customer_curation_path = "C:\\Users\hashim.yousaf\Desktop\JupyterNotebooks\HudiOperations\Curated\sales_per_customers\\"
source_path_historical_customers = "C:\\Users\hashim.yousaf\Desktop\JupyterNotebooks\HudiOperations\Historical\customers\\"
#=================Batch2=================


def get_ssm_parameter(ssm_key_name):
    print("Getting SSM key :: {}".format(ssm_key_name))
    client = boto3.client('ssm')
    response = client.get_parameter( Name=ssm_key_name,WithDecryption=False)
    return eval(response['Parameter']['Value'])

def get_spark_session(app_name='Curation_App'):
    conf = SparkConf() \
        .set('spark.serializer', 'org.apache.spark.serializer.KryoSerializer') \
        .set('spark.sql.hive.convertMetastoreParquet', 'false') \
        .set('spark.port.maxRetries', '40')\
        .set("spark.executor.instances", "4")\
        .set("spark.executor.cores", "2")
    return SparkSession.builder.appName(app_name) \
        .config(conf=conf).enableHiveSupport().getOrCreate()

def get_sales_and_customers_df(spark_session):

    # df_sales = spark_session.read.format("parquet").option("mergeSchema", "true").load(sales_path)
    df_sales = spark_session.read.format("csv").option("header", "true").load(sales_path)
    print("Sales Path ::", sales_path)
    print("Sales COunt :: ", df_sales.count())
    df_sales.show(5)

    # df_customers = spark_session.read.format("parquet").option("mergeSchema", "true").load(customers_path)
    df_customers = spark_session.read.format("csv").option("header", "true").load(customers_path)
    print("Customers Path ::", customers_path)
    print("Customers COunt :: ",df_customers.count())
    df_customers.show(5)
    print("Sales and Customers data fetched for the batch {}".format(batch_id))
    return df_sales, df_customers

def get_old_sales_per_city_and_customer(spark_session, sales_df, customers_df):
    distinct_cities_df = customers_df.select(customers_df.city).distinct()
    distinct_cities_list = [row.city for row in distinct_cities_df.collect()]
    unique_cities_string = ','.join(distinct_cities_list)
    unique_cities_string = "{" + unique_cities_string + "}"
    print("unique_cities_string ::", unique_cities_string)
    sales_per_city = spark_session.read.format("parquet").option("mergeSchema", "true").load(
        target_sales_per_city_curation_path + "city={}".format(unique_cities_string))

    # sales_per_city = spark_session.read.format("parquet").option("mergeSchema", "true").load(target_sales_per_city_curation_path)
    print("Sales per city Path ::", target_sales_per_city_curation_path + "city={}".format(unique_cities_string))
    print("Sales per city COunt :: ", sales_per_city.count())
    sales_per_city.show(5)

    # sales_per_customers = spark_session.read.format("parquet").option("mergeSchema", "true").load(target_sales_per_customer_curation_path)
    # print("Sales per customers Path :: ", target_sales_per_customer_curation_path)
    # print("Sales per customers COunt :: ", sales_per_customers.count())
    # sales_per_customers.show(5)
    # print("Sales/city and Sales/Customers data fetched for the batch {}".format(batch_id))
    return sales_per_city, None

def resolve_dependency_problems(session, df_sales, df_customers):
    # This function will find dependency problem and returns customer after fetching it
    # from historical data.
    print("resolve_dependency_problems")
    customers_joined_df = df_sales.select(df_sales.customer_id).subtract(df_customers.select(df_customers.customer_id))
    count = customers_joined_df.count()
    print("Left join count result :: {}".format(count))
    if count > 0:
        print("We are inside if condition")
        # cust_customer_id = customers_joined_df.select(df_sales.customer_id)
        list_cust = [str(row.customer_id) for row in customers_joined_df.collect()]
        fetch_customers_df = get_dependent_customers_from_historical(session, list_cust)

        fetch_customers_df = fetch_customers_df.drop('timestamp')
        customers_after_union_df = df_customers.unionAll(fetch_customers_df)
        print("Dependency has been resolved till here.")
        # sales_per_city, sales_per_customers = get_sales_per_city_and_customers(df_sales, customers_after_union_df)
        return df_sales, customers_after_union_df
    else:
        # sales_per_city, sales_per_customers = get_sales_per_city_and_customers(df_sales, df_customers)
        return df_sales, df_customers

def get_dependent_customers_from_historical(session, where_clause_ids):
    print("get_dependent_customers_from_historical data fetched")
    df = session.read.format("parquet").option("mergeSchema", "true").load(source_path_historical_customers)
    print("Before count:: ",df.count())

    df = df.where(df.customer_id.isin(where_clause_ids))
    print("After Filter count:: ", df.count())

    multiple_customer_ids_df = df.groupby(df.customer_id).count().where(col('count') > 1)\
        .select(df.customer_id)

    if multiple_customer_ids_df.count() > 0:
        print("There are duplication in ids")
        single_customer_ids_df = df.groupby(df.customer_id).count().where(col('count') == 1) \
            .select(df.customer_id)

        single_lists = [str(row.customer_id) for row in single_customer_ids_df.collect()]
        single_customers_df = df.where(df.customer_id.isin(single_lists))
        print("Single appeared id count :: ", single_customers_df.count())

        dual_df = df.where(df.customer_id.isin(multiple_customer_ids_df)).groupby(df.customer_id) \
            .max('timestamp').withColumnRenamed('max(timestamp)', 'timestamp')
        mutiple_customers_df = df.join(dual_df, (df.customer_id == dual_df.customer_id) & (
                df.timestamp == dual_df.timestamp), 'inner').select([df[xx] for xx in df.columns])

        union_df = single_customers_df.unionAll(mutiple_customers_df)
        print("Union count :: ", union_df.count())
        union_df = union_df.drop('timestamp')
        print("Showing after dropping timestamp column")
        union_df.show()

        return union_df
    else:
        print("There is no duplication in records")
        return df



    df = df.drop('timestamp')
    selected_df = df.where(df.customer_id.isin(where_clause_ids))
    count_value = df.count()
    print("Query count result :: {}".format(count_value))
    if count_value > 0 :
        print("Count of historical fetched data = {}".format(count_value))
        return selected_df
    else:
        raise Exception("Dependency could not be resolved successfully.")

def get_sales_per_city_and_customers(df_sales, df_customers):
    df_joined = df_sales.join(df_customers, df_sales.customer_id == df_customers.customer_id, 'inner')

    sales_per_city = df_joined.select(df_sales.sales_id, df_sales.product_name, df_customers.city,
                                      df_sales.price, df_sales.quantity, df_sales.updated_at, df_sales.is_deleted)
    print("sales_per_city count :: ", sales_per_city.count())
    sales_per_city.show(5)
    sales_per_customers = df_joined.select(df_sales.sales_id, df_sales.customer_id,
                                           df_customers.first_name, df_sales.price, df_sales.updated_at, df_sales.is_deleted)
    print("sales_per_customers count :: ", sales_per_customers.count())
    sales_per_customers.show(5)
    return  sales_per_city, sales_per_customers

def put_curated_data_in_bucket(session, table_df, table_name, target_path):
    if table_name == "sales_per_city":
        table_df.write.mode('overwrite').partitionBy("city").parquet(target_path)
        print("Sales per city has been overwritten")
    elif table_name == "sales_per_customers":
        table_df.write.mode('overwrite').parquet(target_path)
        print("Sales per customers has been overwritten")
    else:
        print("You have to add support of new table in your script")
        
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

def process_deletion_and_upsertion(session, sales_per_city, sales_per_customers, old_sales_per_city, old_sales_per_customer):

    # sales_df =  sales_per_city.filter(sales_per_city.is_deleted == 0)
    # customers_df = sales_per_customers.filter(sales_per_customers.is_deleted == 0)

    sales_id_to_be_deleted = sales_per_city.filter(sales_per_city.is_deleted == 1).select(sales_per_city.sales_id)
    print('sales_id_to_be_deleted  :: ', sales_id_to_be_deleted.show())
    sales_id_to_be_updated = sales_per_city.join(old_sales_per_city,
                                                 sales_per_city.sales_id == old_sales_per_city.sales_id,
                                                 'inner').select(sales_per_city.sales_id)
    print('sales_id_to_be_updated  :: ', sales_id_to_be_updated.show())
    temp_union_df = sales_id_to_be_deleted.union(sales_id_to_be_updated)
    temp_union_list = [row.sales_id for row in temp_union_df.collect()]


    actual_sales_per_city = old_sales_per_city.where(~old_sales_per_city.sales_id.isin(temp_union_list))
    print("actual_sales_per_city count :: ", actual_sales_per_city.count())
    data_to_be_update_df = sales_per_city.where(sales_per_city.sales_id.isin([row.sales_id for row in sales_id_to_be_updated.collect()]))

    print("data_to_be_update_df result")
    data_to_be_update_df.show()

    print("actual_sales_per_city result")
    actual_sales_per_city.show()

    data_to_be_update_df = data_to_be_update_df.drop(data_to_be_update_df.is_deleted)

    actual_sales_per_city = actual_sales_per_city.union(data_to_be_update_df)

    return actual_sales_per_city

def main():
    spark_session = get_spark_session()
    print("Spark session created")
    sales_df, customers_df = get_sales_and_customers_df(spark_session)

    is_first_load = "false"

    if is_first_load != "true":
        sales_df, customers_df = resolve_dependency_problems(spark_session, sales_df, customers_df)
        sales_per_city, sales_per_customers = get_sales_per_city_and_customers(sales_df, customers_df)
        old_sales_per_city, old_sales_per_customers = get_old_sales_per_city_and_customer(spark_session,sales_df, customers_df)
        sales_per_city_to_insert = process_deletion_and_upsertion(spark_session,
                                                                       sales_per_city,
                                                                       sales_per_customers,
                                                                       old_sales_per_city,
                                                                       old_sales_per_customers)
        # put_curated_data_in_bucket(spark_session, sales_per_city_to_insert, "sales_per_city", target_sales_per_city_curation_path)
    else:
        sales_per_city, sales_per_customers = get_sales_per_city_and_customers(sales_df, customers_df)
        sales_per_city = sales_per_city.drop(sales_per_city.is_deleted)
        # put_curated_data_in_bucket(spark_session, sales_per_city, "sales_per_city", target_sales_per_city_curation_path)

    # put_curated_data_in_bucket(sales_per_customers, "sales_per_customers", target_curation_path)

if __name__ == '__main__':
    main()

# get sales and customer data from staging batch
# see if there is any dependency problem there but if it's not the first batch
# if there is not any dependency then put it in curated table
# else fetch customer_id from sales which are not in customers table
# and query it in historical customer table
# and now union this dataset of customers with actual customers
# and join it and make curated table.
