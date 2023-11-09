import json
import boto3
import logging
from pprint import pprint

# Initialize the Boto3 CloudFormation and CloudWatch Logs clients
cf_client = boto3.client('cloudformation')
logs_client = boto3.client('logs')

# Set up logging for the Lambda function
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Stacks status to skip checking 
invalidStatusList = ["DELETE_COMPLETE", "CREATE_FAILED", "DELETE_IN_PROGRESS", "IMPORT_ROLLBACK_FAILED", "ROLLBACK_COMPLETE"]

#Stack status filter list
stackFilterList = [ "CREATE_COMPLETE", "UPDATE_COMPLETE", "UPDATE_ROLLBACK_COMPLETE"]

#Search tags
search_tags = [ "Owner" ]

def lambda_handler(event, context):
    # Stack filter list
    # stackFilterList = event.get("stack_filters") 
    
    # Tags to search for, taken from Event json
    # search_tags = event.get("search_tags")
    
    # string msg used to when tag searched tag is not found
    tagNotFoundMsg = ": Search Tag not found in this stack"
    # Variable to store number of stacks in total
    totalStacksProcessed = 0
    # Variable to store count of deleted stacks
    totalDeletedStacks = 0
    # Variable to store number of stacks without all the given tags
    totalStacksWithoutTags = 0
    # List all the stacks with filters
    list_stacks_response = cf_client.list_stacks(StackStatusFilter=stackFilterList)
    nextTokenVar = ""
    while(nextTokenVar != None):
        nextTokenVar = list_stacks_response.get("NextToken")
        # Log number of stacks in the response and add total number of stacks
        logger.info("Number of stacks in this response: " + str(len(list_stacks_response.get("StackSummaries"))))
        totalStacksProcessed += len(list_stacks_response.get("StackSummaries"))
        try:
            # Iterate over each stack in the response
            for eachStack in list_stacks_response.get("StackSummaries"):
                # log the details of current stack
                tempStackName = eachStack.get("StackName")
                logger.info("StackName: "+tempStackName+", StackId: "+eachStack.get("StackId"))
                logger.info("Current status of the stack: " + eachStack.get("StackStatus"))
                # boolean value used to check if all the tags were found in the current stack
                tagFlag = False
                # Check if the stack is in a invalid state
                if(eachStack.get("StackStatus") in invalidStatusList):
                    logger.info("Invalid stack status !!")
                    # if yes, continue to the next stack
                    continue
                # Get description of current Valid state stack using tempStackName
                eachStackMetadata = cf_client.describe_stacks(StackName=tempStackName)['Stacks'][0]
                # if there are no tags, branch out
                if(len(eachStackMetadata.get("Tags"))==0):
                    tagFlag = False
                    logger.info("No tags in this stack")
                else:
                    # else, read all the tags in the stack and see of all the search keys are present
                    # if ALL THE KEYS are present, then tagFlag is true, or else, it is sest to be false
                    # log the same
                    for eachDict in eachStackMetadata.get("Tags"):
                        for eachSearchTag in search_tags:
                            if eachDict.get("Key") == eachSearchTag:
                                logger.info(eachSearchTag + " tag found !!")
                                tagFlag = True
                            else:
                                tagFlag = False
                                logger.info(eachSearchTag + tagNotFoundMsg)
                                
                # if all the search tags are found, then list all the resources
                if(tagFlag == True):     
                    resource_response = cf_client.list_stack_resources(StackName=tempStackName)
                    # Loop through the stack's resources and delete them
                    for eachResource in resource_response['StackResourceSummaries']:
                        resource_id = eachResource['PhysicalResourceId']
                        resource_type = eachResource['ResourceType']
                        logger.info("Resource Type: (" + resource_type + "), Resource Identifier: (" + resource_id + ")")
                # else, delete the stack
                # for now deleteStack command is commented out for testing purpose
                else:
                    totalStacksWithoutTags +=1
                    logger.info("Deleting stack: " + tempStackName)
                    totalDeletedStacks += 1
                    # ********* USE WITH CAUTION *************
                    # deleteCallResponse = cf_client.delete_stack(StackName=tempStackName)
                    # if(deleteCallResponse.get(HTTPStatusCode) == 200):
                    #     logger.info("Deletion initiation successful")
                    # else:
                    #     logger.error("An error occurred while trying to delete the stack: "+tempStackName)
                    #     logger.info(deleteCallResponse)
            
            # Implementing pagination for stack listing
            if nextTokenVar != None:
                list_stacks_response = cf_client.list_stacks(NextToken=nextTokenVar, StackStatusFilter=stackFilterList)
            else:
                return {
                    'statusCode': 200,
                    'processed_stacks': totalStacksProcessed,
                    'stacks_without_tags': totalStacksWithoutTags,
                    'deleted_stacks': totalDeletedStacks
                    }

        except Exception as e:
            logger.error(f"An error occurred: {str(e)}")
            return {
                'statusCode': 500,
                'body': json.dumps(f'An error occurred: {str(e)}')
            }
