import json
import boto3
from pprint import pprint
import os
import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    
    clustersWithoutTags = 0
    deletedClusters = 0
    pausedClusters = 0
    snapshotsCreated = 0

    # PauseFlag set to True if u want to Pause it, or else it will delete
    pauseFlag = True
    
    # invalid status list
    invalidStatusList = ["deleting", "hardware-failure", "modifying", "cancelling-resize"]
    
    # Tags to check
    tagsToCheck = event.get("tagsToCheck")
    
    rs_client = boto3.client("redshift")
    response = rs_client.describe_clusters()
    markerValue = ""
    
    while (markerValue != None):
        # Handle pagination and use the current marker value
        if("Marker" in response):
                markerValue = response["Marker"]
        else:
                markerValue = None
        
        # Cluster Handling
        for eachCluster in response["Clusters"]:
            logger.info("Cluster Identifier: " + eachCluster["ClusterIdentifier"])
            logger.info("Cluster Status: " + eachCluster["ClusterStatus"])
            
            # Check for invalid status from a list
            if(eachCluster["ClusterStatus"] in invalidStatusList):
                logger.info("Invalid cluster status !!")
                continue
            else:
                # Delete cluster if all the tags are not present
                tagFlag = False
                if len(eachCluster["Tags"]) != 0:
                    for eachTagDict in eachCluster["Tags"]:
                        if(eachTagDict["Key"] in tagsToCheck):
                            tagFlag = True
                        else:
                            tagFlag = False
                else:
                    tagFlag = False
                    
                if(tagFlag == False):
                    if(pauseFlag == True):

                        # Check if this cluster has atleast one snapshot
                        snapshotCount = 0
                        snapshotResponse = rs_client.describe_cluster_snapshots(
                            ClusterIdentifier=eachCluster["ClusterIdentifier"],
                            ClusterExists=True)
                            try:
                                snapshotCount = len(snapshotResponse["Snapshots"])
                            except Exception as e:
                                snapshot = 0

                        # If no snapshots are found, create one snapshot
                        if(snapshot==0):
                            snapshotCreationResponse = rs_client.create_cluster_snapshot(
                                SnapshotIdentifier=str(eachCluster["ClusterIdentifier"] + "-snapshot"),
                                ClusterIdentifier=eachCluster["ClusterIdentifier"])
                            logger.info("Snapshot Created for " + eachCluster["ClusterIdentifier"])
                            snapshotsCreated += 1

                        # Pause the clusters
                        pauseResponse = rs_client.pause_cluster(
                            ClusterIdentifier=eachCluster["ClusterIdentifier"],
                            )
                        
                        logger.info("Paused Cluster: " + eachCluster["ClusterIdentifier"])
                        pausedClusters += 1
                    else:
                        # Uncomment the following block to actually delete the clusters
                    
                        # skipFinalSnapshot set to True if u don't want Final Snapshot, else False
                        # skipFinalSnapshot = False
                        # if(skipFinalSnapshot == True):
                        #     deleteResponse = rs_client.delete_cluster(
                        #         ClusterIdentifier=eachCluster["ClusterIdentifier"],
                        #         SkipFinalClusterSnapshot = skipFinalSnapshot)
                        # else:
                        #     deleteResponse = rs_client.delete_cluster(
                        #         ClusterIdentifier=eachCluster["ClusterIdentifier"],
                        #         SkipFinalClusterSnapshot = skipFinalSnapshot,
                        #         FinalClusterSnapshotIdentifier = str(eachCluster["ClusterIdentifier"] + "-snapshot"),
                        #         FinalClusterSnapshotRetentionPeriod = 10)
                
                        logger.info("Deleted Cluster: " + eachCluster["ClusterIdentifier"])
                        deletedClusters += 1
                
                else:
                    logger.info("All tags found in cluster: " + eachCluster["ClusterIdentifier"])
                    clustersWithoutTags += 1
                
        # Pagination Handling
        if(markerValue != "" and markerValue != None):
            response = rs_client.describe_clusters(Marker=markerValue)
        else:
            markerValue = None
    
    return {
        'clustersWithoutTags': clustersWithoutTags,
        'deletedClusters': deletedClusters,
        'snapshotsCreated': snapshotsCreated,
        'pausedClusters': pausedClusters
    }
