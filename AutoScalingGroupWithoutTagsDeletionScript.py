import json
import boto3
import os
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    autoscaling_client = boto3.client('autoscaling')

    instancesWithoutTags = 0
    totalInstances = 0
    deletedInstances = 0
    stoppedInstances = 0

    # Tags to exclude from Auto Scaling Group instances
    excludedTagKeys = {"Owner"}
    
    pauseFlag = True

    next_token = None

    while True:
        # Use NextToken for pagination
        autoscaling_groups = autoscaling_client.describe_auto_scaling_groups(NextToken=next_token) if next_token else autoscaling_client.describe_auto_scaling_groups()

        for group in autoscaling_groups['AutoScalingGroups']:
            totalInstances += group['DesiredCapacity']
            for instance in group['Instances']:
                instance_id = instance['InstanceId']
                logger.info(f"Auto Scaling Group Instance ID: {instance_id}")

                # Check if the instance has excluded tag keys
                hasExcludedTags = any(tag['Key'] in excludedTagKeys for tag in instance.get('Tags', []))

                if hasExcludedTags:
                    logger.info(f"Instance in Auto Scaling Group has excluded tags, skipping: {instance_id}")
                else:
                    instancesWithoutTags += 1
                    if instance['LifecycleState'] == 'InService':
                        if not pauseFlag:
                            # Terminate (delete) the instance in the Auto Scaling Group
                            autoscaling_client.terminate_instance_in_auto_scaling_group(
                                InstanceId=instance_id,
                                ShouldDecrementDesiredCapacity=False
                            )
                            logger.info(f"Terminated Auto Scaling Group Instance: {instance_id}")
                            deletedInstances += 1
                        else:
                            # Detach the instance from the Auto Scaling Group
                            autoscaling_client.detach_instances(
                                InstanceIds=[instance_id],
                                AutoScalingGroupName=group['AutoScalingGroupName'],
                                ShouldDecrementDesiredCapacity=True
                            )
                            logger.info(f"Detached Auto Scaling Group Instance: {instance_id}")
                            stoppedInstances += 1

        # Check if there is more data to retrieve
        next_token = autoscaling_groups.get('NextToken')
        if not next_token:
            break

    return {
        'instancesWithoutTags': instancesWithoutTags,
        'deletedInstances': deletedInstances,
        'stoppedInstances': stoppedInstances,
        'totalInstances': totalInstances
    }
