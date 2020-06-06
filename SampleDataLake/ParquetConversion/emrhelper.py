#!/usr/bin/python

"""This script has an AWS EMR Helper Class"""

import time
import botocore
import boto3

REGION = boto3.session.Session().region_name
S3_SCRIPT_RUNNER_JAR_PATH = 's3://' + REGION \
                            + '.elasticmapreduce/libs/script-runner/script-runner.jar'


class EMRHelper:
    """AWS EMR Helper Class having functions for AWS EMR"""

    def __init__(self, emr_client):
        """Object Initialization Function"""

        self.emr_client = emr_client

    def get_emr_step_details(self, emr_id, step_id, counter=1):
        """Gets Details of a Step on an EMR Cluster"""

        try:
            response = self.emr_client.describe_step(
                ClusterId=emr_id,
                StepId=step_id
            )

            return response
        except botocore.exceptions.ClientError as exception:
            if exception.response['Error']['Code'] == 'ThrottlingException':
                while counter <= 5:
                    print("ThrottlingException, Retrying.....")
                    print("Attempt No." + str(counter))
                    time.sleep(counter)
                    counter += 1
                    return self.get_emr_step_details(emr_id, step_id, counter)

                raise
            else:
                raise

    def get_emr_cluster_state(self, emr_id, counter=1):
        """Gets Details of a Step on an EMR Cluster"""

        try:
            response = self.emr_client.describe_cluster(
                ClusterId=emr_id
            )
            return response['Cluster']['Status']['State']

        except botocore.exceptions.ClientError as exception:
            if exception.response['Error']['Code'] == 'ThrottlingException':
                while counter <= 5:
                    print("ThrottlingException, Retrying.....")
                    print("Attempt No." + str(counter))
                    time.sleep(counter)
                    counter += 1
                    return self.get_emr_cluster_state(emr_id, counter)

                raise
            else:
                raise

    def get_latest_emr_step(self, emr_id, counter=1):
        try:
            response = self.emr_client.list_steps(
                ClusterId=emr_id
            )
            if len(response['Steps']) is 0:
                return None
            else:
                latest_step = response['Steps'][0]
            return latest_step
        except botocore.exceptions.ClientError as exception:
            if exception.response['Error']['Code'] == 'ThrottlingException':
                while counter <= 5:
                    print("ThrottlingException, Retrying.....")
                    print("Attempt No." + str(counter))
                    time.sleep(counter)
                    counter += 1
                    return self.get_latest_emr_step(emr_id, counter)
                raise
            else:
                raise

    def get_all_completed_steps(self, emr_id, counter=1):
        all_steps = []
        try:
            response = self.emr_client.list_steps(
                ClusterId=emr_id,
                StepStates=['COMPLETED']
            )
            all_steps.append(response)
            while "Marker" in response:
                response = self.emr_client.list_steps(
                    ClusterId=emr_id,
                    StepStates=['COMPLETED'],
                    Marker=response["Marker"]
                )
                all_steps.append(response)
        except botocore.exceptions.ClientError as exception:
            if exception.response['Error']['Code'] == 'ThrottlingException':
                while counter <= 5:
                    print("ThrottlingException, Retrying.....")
                    print("Attempt No." + str(counter))
                    time.sleep(counter)
                    counter += 1
                    return self.get_all_completed_steps(emr_id, counter)

                raise
            else:
                raise
        return all_steps

    def check_hadoop_steps(self, emr_id, counter=1):
        """Checks if Required Hadoop Driver Steps are already completed on the cluster"""
        try:
            all_steps = self.get_all_completed_steps(emr_id)
            all_steps_names = []
            for item in all_steps:
                for step in item['Steps']:
                    all_steps_names.append(step['Name'])
            if 'Hadoop Step Debugging' in all_steps_names and \
                    'Install Custom Drivers For Sqoop and Script Runner' in all_steps_names:
                print(
                    'Hadoop Step Debugging and Install Custom Drivers For Sqoop and Script Runner found')
                return True
            else:
                return False

        except botocore.exceptions.ClientError as exception:
            if exception.response['Error']['Code'] == 'ThrottlingException':
                while counter <= 5:
                    print("ThrottlingException, Retrying.....")
                    print("Attempt No." + str(counter))
                    time.sleep(counter)
                    counter += 1
                    return self.check_hadoop_steps(emr_id, counter)

                raise
            else:
                raise

    def list_live_emr_clusters(self, counter=1):
        """Gets List of Live EMR Clusters"""

        try:
            response = self.emr_client.list_clusters(
                ClusterStates=['STARTING',
                               'BOOTSTRAPPING', 'RUNNING', 'WAITING']
            )

            return response
        except botocore.exceptions.ClientError as exception:
            if exception.response['Error']['Code'] == 'ThrottlingException':
                while counter <= 5:
                    print("ThrottlingException, Retrying.....")
                    print("Attempt No." + str(counter))
                    time.sleep(counter)
                    counter += 1
                    return self.list_live_emr_clusters(counter)

                raise
            else:
                raise

    def get_cluster_status(self, cluster_id, counter=1):
        """Gets List of Live EMR Clusters"""

        try:
            response = self.emr_client.describe_cluster(
                ClusterId=cluster_id
            )

            return response
        except botocore.exceptions.ClientError as exception:
            if exception.response['Error']['Code'] == 'ThrottlingException':
                while counter <= 5:
                    print("ThrottlingException, Retrying.....")
                    print("Attempt No." + str(counter))
                    time.sleep(counter)
                    counter += 1
                    return self.get_cluster_status(cluster_id, counter)

                raise
            else:
                raise

    def add_emr_step(self, emr_id, step_name, action_on_failure, job_args, counter=1):
        """Adds Step on an EMR Cluster"""

        try:
            response = self.emr_client.add_job_flow_steps(
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
                    return self.add_emr_step(emr_id, step_name, action_on_failure, job_args,
                                             counter)

                raise
            else:
                raise

    def get_emr_name_by_id(self, job_flow_id, counter=1):
        """Gets the name of the EMR for a given job_flow_id"""

        try:
            response = self.emr_client.describe_cluster(
                ClusterId=job_flow_id
            )

            return response['Cluster']['Name']
        except botocore.exceptions.ClientError as exception:
            if exception.response['Error']['Code'] == 'ThrottlingException':
                while counter <= 5:
                    print("ThrottlingException, Retrying.....")
                    print("Attempt No." + str(counter))
                    time.sleep(counter)
                    counter += 1
                    return self.get_emr_name_by_id(job_flow_id, counter)
                raise
            else:
                raise

    def terminate_emr_by_id(self, job_flow_id, counter=1):
        """Terminates an EMR having the passed job_flow_id"""

        try:
            self.emr_client.terminate_job_flows(
                JobFlowIds=[job_flow_id]
            )
        except botocore.exceptions.ClientError as exception:
            if exception.response['Error']['Code'] == 'ThrottlingException':
                while counter <= 5:
                    print("ThrottlingException, Retrying.....")
                    print("Attempt No." + str(counter))
                    time.sleep(counter)
                    counter += 1
                    return self.terminate_emr_by_id(job_flow_id, counter)
                raise
            elif exception.response['Error']['Code'] == 'ValidationError':
                # No EMR with job_flow_id found - safely exit this exception
                return None

    def get_master_instance_id(self, emr_cluster_id, counter=1):
        """Provided an EMR cluster id, this will return EMR's master node EC2 instance id"""

        try:
            response = self.emr_client.list_instances(
                ClusterId=emr_cluster_id,
                InstanceGroupTypes=[
                    'MASTER'
                ],
                InstanceStates=[
                    'RUNNING'
                ]
            )
            return response['Instances'][0]['Ec2InstanceId']
        except botocore.exceptions.ClientError as exception:
            if exception.response['Error']['Code'] == 'ThrottlingException':
                while counter <= 5:
                    print("ThrottlingException, Retrying.....")
                    print("Attempt No." + str(counter))
                    time.sleep(counter)
                    counter += 1
                    return self.get_master_instance_id(emr_cluster_id, counter)
                raise exception
            else:
                raise exception
