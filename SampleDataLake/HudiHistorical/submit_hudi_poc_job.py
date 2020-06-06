import time
import botocore
import boto3

S3_SCRIPT_RUNNER_JAR_PATH = 'command-runner.jar'

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
def submit_hudi_job(description):
    job_args =  ['spark-submit','--jars',
                 's3://air-product-training-datalake/hashim-yousaf/jars/commons-codec_commons-codec-1.11.jar,s3://air-product-training-datalake/hashim-yousaf/jars/org.apache.hudi_hudi-spark-bundle_2.11-0.5.1-incubating.jar,s3://air-product-training-datalake/hashim-yousaf/jars/commons-logging_commons-logging-1.2.jar,s3://air-product-training-datalake/hashim-yousaf/jars/org.apache.httpcomponents_httpclient-4.5.9.jar,s3://air-product-training-datalake/hashim-yousaf/jars/org.apache.httpcomponents_httpcore-4.4.11.jar,s3://air-product-training-datalake/hashim-yousaf/jars/org.apache.spark_spark-avro_2.11-2.4.4.jar,s3://air-product-training-datalake/hashim-yousaf/jars/org.spark-project.spark_unused-1.0.0.jar',
                 '/home/hadoop/hudi_poc.py']
    response  = add_emr_step("j-2WU9A63L2NLFU", description, "CONTINUE", job_args)
    print(response)


if __name__ == '__main__':
    submit_hudi_job("Inser with bulk-insert2 mode job")