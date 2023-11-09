import json
import boto3
import os
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    ec2_client = boto3.client('ec2')

    instancesWithoutTags = 0
    totalInstances = 0
    deletedInstances = 0
    stoppedInstances = 0

    # Tags to exclude from EC2 instances
    excludedTagKeys = {"Owner"}
    
    pauseFlag = True

    next_token = None

    while True:
        # Use NextToken for pagination
        ec2_instances = ec2_client.describe_instances(NextToken=next_token) if next_token else ec2_client.describe_instances()

        for reservation in ec2_instances['Reservations']:
            totalInstances += len(reservation['Instances'])
            for instance in reservation['Instances']:
                instance_id = instance['InstanceId']
                logger.info(f"EC2 Instance ID: {instance_id}")

                # Check if the instance has excluded tag keys
                hasExcludedTags = any(tag['Key'] in excludedTagKeys for tag in instance.get('Tags', []))

                if hasExcludedTags:
                    logger.info(f"Instance has excluded tags, skipping: {instance_id}")
                else:
                    instancesWithoutTags += 1
                    if instance['State']['Name'] == 'running':
                        if not pauseFlag:
                            # Terminate (delete) the EC2 instance
                            ec2_client.terminate_instances(InstanceIds=[instance_id])
                            logger.info(f"Terminated EC2 Instance: {instance_id}")
                            deletedInstances += 1
                        else:
                            # Stop the EC2 instance
                            ec2_client.stop_instances(InstanceIds=[instance_id])
                            logger.info(f"Stopped EC2 Instance: {instance_id}")
                            stoppedInstances += 1

        # Check if there is more data to retrieve
        next_token = ec2_instances.get('NextToken')
        if not next_token:
            break

    return {
        'instancesWithoutTags': instancesWithoutTags,
        'deletedInstances': deletedInstances,
        'stoppedInstances': stoppedInstances,
        'totalInstances': totalInstances
    }
