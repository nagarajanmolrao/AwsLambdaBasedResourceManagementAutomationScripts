import boto3
import logging
import json
logger = logging.getLogger()
logger.setLevel(logging.INFO)

region = 'ap-south-1'
# Add all the tag names to be checked
tagNamesToCheck = ["Owner"]

# Delete Flag, True if you want to delete RDS instances
deleteFlag = False

# SkipFinalSnapShot Flag, if you don't want final snapshot, the value is True
skipFinalSnapshot = True

# Delete automated backups flag, True if you want to delete
deleteAutomatedBackupsFlag = True


def rdsMethod():
    stoppedRds = 0
    deletedRds = 0
    rds = boto3.client('rds', region_name=region)
    response = rds.describe_db_instances()
    filteredRdsInstancesIds = []
    for eachDbInstance in response["DBInstances"]:
        # logger.info(eachDbInstance["DBInstanceIdentifier"])
        tempListOfKeys = []
        for eachTag in eachDbInstance["TagList"]:
            tempListOfKeys.append(eachTag["Key"])
        # filtering rds instances without "Owner" key, regardless of "Owner" value
        for eachTagNameToBeChecked in tagNamesToCheck:
            if (eachTagNameToBeChecked not in tempListOfKeys) and (eachDbInstance["DBInstanceIdentifier"] not in filteredRdsInstancesIds):
                    filteredRdsInstancesIds.append(eachDbInstance["DBInstanceIdentifier"])
                    
        # logger.info(filteredRdsInstanceIds)
        for eachRdsInstance in filteredRdsInstancesIds:
            # stop RDS instances if deleteFlag is False
            if(deleteFlag == False):
                try:
                    rds.stop_db_instance(DBInstanceIdentifier=eachRdsInstance)
                except Exception as e:
                    logger.error(e)
                    break
                logger.info('Stopped your RDS instance: ' + str(eachRdsInstance))
                stoppedRds += 1
            # Delete RDS instances if deleteFlag is True
            else:
                if (skipFinalSnapshot == True):
                    try:
                        rds.delete_db_instance(
                            DBClusterIdentifier=eachRdsInstance,
                            SkipFinalSnapshot=True,
                            DeleteAutomatedBackups=deleteAutomatedBackupsFlag)
                    except Exception as e:
                        logger.error(e)
                        break
                    logger.info("Deleted your RDS instance - NO Snapshot : " + str(eachRdsInstance))
                    deletedRds += 1
                else:
                    try:
                        rds.delete_db_instance(
                            DBClusterIdentifier=eachRdsInstance,
                            SkipFinalSnapshot=False,
                            FinalDBSnapshotIdentifier=str(eachDbInstance + "-finalSnapshot"),
                            DeleteAutomatedBackups=deleteAutomatedBackupsFlag)
                    except Exception as e:
                        logger.error(e)
                        break
                    logger.info("Deleted your RDS instance - Final Snapshot Taken : " + str(eachRdsInstance))
                    deletedRds += 1
                        
    return( {
        "Stopped RDS_Instances": stoppedRds,
        "Deleted RDS_Instances": deletedRds,
    })

def lambda_handler(event, context):
    return rdsMethod()
    
