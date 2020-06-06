import time
import botocore
import boto3

S3_SCRIPT_RUNNER_JAR_PATH = 'command-runner.jar'
base_bucket_name = "air-product-training-datalake"
staging_bukcet = 'hashim-yousaf/staging-data/'
base_ssm_key = "/ap/tables/hashim/"
tablename_sales = "sales"
tablename_customers = "customers"


def read_object(entity_name, last_processed_batch, prefix_value=staging_bukcet):
    client = boto3.client('s3')
    if last_processed_batch == "-1":
        response = client.list_objects_v2(
            Bucket='air-product-training-datalake',
            Prefix=prefix_value)
    else:
        response = client.list_objects_v2(
            Bucket='air-product-training-datalake',
            Prefix=prefix_value,
            StartAfter= last_processed_batch)


    bucket_list = []
    for bucket in response['Contents']:
        if bucket['Key'] != prefix_value:
            if bucket['Key'].split("/")[3] == entity_name:
                bucket_list.append(bucket['Key'].split("/")[2])

    bucket_list = list(set(bucket_list))
    if last_processed_batch != "-1":
        bucket_list = list(filter(lambda x: x > last_processed_batch, bucket_list))
    bucket_list.sort()
    bucket_results = list(map(lambda x:  "s3://{}/{}{}/{}/"
                              .format(base_bucket_name, staging_bukcet, x, entity_name), bucket_list))
    return bucket_results

def get_ssm_parameter(ssm_key_name):
    client = boto3.client('ssm')
    response = client.get_parameter( Name=ssm_key_name,WithDecryption=False)
    return eval(response['Parameter']['Value'])


def add_emr_step(emr_id, step_name, action_on_failure, job_args, counter=1):
    """Adds Step on an EMR Cluster"""
    emr_client = boto3.client('emr')
    try:
        response = emr_client.add_job_flow_steps(
            JobFlowId=emr_id,
            Steps=[
                {
                    'Name': step_name,
                    'ActionOnFailure': action_on_failure,
                    'HadoopJarStep': {
                        'Jar': S3_SCRIPT_RUNNER_JAR_PATH,
                        'Args': job_args
                    }
                }
            ]
        )
        return response
    except botocore.exceptions.ClientError as exception:
        if exception.response['Error']['Code'] == 'ThrottlingException':
            while counter <= 10:
                print("ThrottlingException, Retrying.....")
                print("Attempt No." + str(counter))
                time.sleep(counter)
                counter += 1
                return add_emr_step(emr_id, step_name, action_on_failure, job_args,
                                         counter)
            raise
        else:
            raise
# spark-submit --master yarn --deploy-mode cluster --jars commons-codec_commons-codec-1.11.jar,org.apache.hudi_hudi-spark-bundle_2.11-0.5.1-incubating.jar,commons-logging_commons-logging-1.2.jar,org.apache.httpcomponents_httpclient-4.5.9.jar,org.apache.httpcomponents_httpcore-4.4.11.jar,org.apache.spark_spark-avro_2.11-2.4.4.jar,org.spark-project.spark_unused-1.0.0.jar HudihelloWorld.py
def submit_hudi_job(table_name, last_processed_timestamp, list_of_files):
    job_args =  ['spark-submit','--jars',
                 's3://air-product-training-datalake/hashim-yousaf/jars/commons-codec_commons-codec-1.11.jar,s3://air-product-training-datalake/hashim-yousaf/jars/org.apache.hudi_hudi-spark-bundle_2.11-0.5.1-incubating.jar,s3://air-product-training-datalake/hashim-yousaf/jars/commons-logging_commons-logging-1.2.jar,s3://air-product-training-datalake/hashim-yousaf/jars/org.apache.httpcomponents_httpclient-4.5.9.jar,s3://air-product-training-datalake/hashim-yousaf/jars/org.apache.httpcomponents_httpcore-4.4.11.jar,s3://air-product-training-datalake/hashim-yousaf/jars/org.apache.spark_spark-avro_2.11-2.4.4.jar,s3://air-product-training-datalake/hashim-yousaf/jars/org.spark-project.spark_unused-1.0.0.jar',
                 '/home/hadoop/hudi_historical_data.py',
                 base_ssm_key + table_name, str(list_of_files)]
    response  = add_emr_step("j-TNMST10OZPNN", "Historical :: {} job for last "
                                                "processed Timestamp{}"
                             .format(table_name, last_processed_timestamp), "CONTINUE", job_args)
    print(response)

def main():
    list_tables =  [tablename_customers, tablename_sales]
    for table_name in list_tables:
        ssm_key_name = base_ssm_key + table_name
        table_config = get_ssm_parameter(ssm_key_name)
        entity_name = table_config["table_name"]
        last_processed_batch = table_config["processed_batch"]
        list_of_paths  = read_object(entity_name, last_processed_batch)
        print(list_of_paths)
        print("Submitting job on EMR")
        if len(list_of_paths) > 0:
            submit_hudi_job(entity_name, last_processed_batch, list_of_paths)
        else:
            print("\nThere is no incremental load\n")


if __name__ == '__main__':
    main()


