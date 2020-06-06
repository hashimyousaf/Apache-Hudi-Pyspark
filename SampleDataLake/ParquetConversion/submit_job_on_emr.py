import time
import botocore
import boto3


REGION = boto3.session.Session().region_name
S3_SCRIPT_RUNNER_JAR_PATH = 'command-runner.jar'
ssm_key_name = '/ap/tables/kmskey'
landing_bukcet = 'hashim-yousaf/landing-data/'


def get_ssm_parameter():
    client = boto3.client('ssm')
    response = client.get_parameter( Name=ssm_key_name,WithDecryption=False)
    return eval(response['Parameter']['Value'])

def read_object(entity_name, prefix_value = landing_bukcet):
    client = boto3.client('s3')
    response = client.list_objects_v2(
        Bucket='air-product-training-datalake',
        Prefix= prefix_value)

    bucket_list = []
    for bucket in response['Contents']:
        bucket_list.append(bucket['Key'])
    bucket_list.sort()
    bucket_results = list(filter(lambda x: "/{}/".format(entity_name).lower() in x, bucket_list))
    return bucket_results

def get_next_list_of_timestamps_to_be_processed(entiy_name, last_processed):
    all_timestamp_objects =  read_object(entiy_name)
    all_timestamp_objects.sort()
    next_timestamps_to_be_processed = list(filter(lambda x: x > last_processed))
    print("next_timestamps_to_be_processed are {}".format(next_timestamps_to_be_processed))


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

def submit_conversion_job(table_name, timestamp, list_of_files):
    job_args =  ['spark-submit', '/home/hadoop/parquet_conversion.py',
                 table_name, timestamp, str(list_of_files)]
    response  = add_emr_step("j-TNMST10OZPNN", "{} job for {}"
                             .format(table_name, timestamp), "CONTINUE", job_args)
    print(response)

def main():
    tables_dict = get_ssm_parameter()
    for (table_name, timestamp) in tables_dict.items():
        if timestamp == '-1':
            # Read every bucket from landing-data and go into folder named table_name and convert it into parquet file
            print("{} has timestamp value {}".format(table_name, timestamp))
            list_of_files = read_object(table_name)
            print("EntityName = {} and timestamps to be processed are {}".format(table_name, list_of_files))
            response = submit_conversion_job(table_name, timestamp, list_of_files)
        else:
            list_of_files = read_object(table_name)
            path_to_be_processed = list(filter(lambda x:  x.split('/')[2] > timestamp, list_of_files))
            print("EN = {} and latest TS to be processed are {}".format(table_name, list_of_files))
            print("******"*100)
            print("This is filtered list {}".format(path_to_be_processed))
            if len(path_to_be_processed) > 0:
                response = submit_conversion_job(table_name, timestamp, path_to_be_processed)
            else:
                print("There is no data in landing to convert into parquet.")
                response = '[]'

        print("Step Submitted response ", response)
    # read only bucket with timestamp value with timestamp and go into folder
    # table_name and convert csv into parquet
if __name__ == '__main__':
    main()

    # list(filter(lambda x: '/sales/' in x, bucket_list))