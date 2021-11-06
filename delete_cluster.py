import pandas as pd
import boto3
import json
import psycopg2

from botocore.exceptions import ClientError
import configparser

from random import random
import threading
import time

import utility

# Tracking Cluster Creation Progress
progress = 0
cluster_status = ''
cluster_event = threading.Event()

def initialize():
    """
    This function starts the delete_cluster function. 
  
    Parameters: 
    NONE
  
    Returns: 
    None
    """    
    
    # Get the config properties from dwh.cfg file
    config = configparser.ConfigParser()
    config.read_file(open('./config.cfg'))

    KEY                    = config.get('AWS','KEY')
    SECRET                 = config.get('AWS','SECRET')
 
    CLUSTER_TYPE           = config.get("CLUSTER","CLUSTER_TYPE")
    NUM_NODES              = config.get("CLUSTER","NUM_NODES")
    NODE_TYPE              = config.get("CLUSTER","NODE_TYPE")

    CLUSTER_NAME           = config.get("CLUSTER","CLUSTER_NAME")
    DB_NAME                = config.get("CLUSTER","DB_NAME")
    DB_USER                = config.get("CLUSTER","DB_USER")
    DB_PASSWORD            = config.get("CLUSTER","DB_PASSWORD")
    DB_PORT                = config.get("CLUSTER","DB_PORT")

    IAM_ROLE_NAME          = config.get("CLUSTER", "IAM_ROLE_NAME")
    
    bucket_name            = config.get("S3", "BUCKET")
    

    df = pd.DataFrame({"Param":
                    ["CLUSTER_TYPE", "NUM_NODES", "NODE_TYPE", "CLUSTER_NAME", "DB_NAME", "DB_USER", "DB_PASSWORD", "DB_PORT", "IAM_ROLE_NAME"],
                "Value":
                    [CLUSTER_TYPE, NUM_NODES, NODE_TYPE, CLUSTER_NAME, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT, IAM_ROLE_NAME]
                })

    print(df)


    # Intializing the AWS resources    
    ec2 = boto3.resource('ec2',
                        region_name="us-west-2",
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET
                        )

    s3 = boto3.resource('s3',
                        region_name="us-west-2",
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET
                    )

    iam = boto3.client('iam',aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET,
                        region_name='us-west-2'
                    )

    redshift = boto3.client('redshift',
                        region_name="us-west-2",
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET
                        )

    
    myClusterProps = get_cluster_properties(redshift, CLUSTER_NAME)
    
    if myClusterProps[0] != '':
        #print(myClusterProps)
        prettyRedshiftProps(myClusterProps[0])
        ENDPOINT = myClusterProps[1]
        ROLE_ARN = myClusterProps[2]
        print('ENDPOINT = {}'.format(ENDPOINT))
        print('ROLE_ARN = {}'.format(ROLE_ARN))
    print('1. Delete Redshit Cluster')   
    delete_cluster(redshift, CLUSTER_NAME)
  
    #thread = threading.Thread(target=check_cluster_status)
    thread = threading.Thread(target=lambda : check_cluster_status(redshift, CLUSTER_NAME, 'delete', 'none'))
    #thread = threading.Thread(target=lambda : check_cluster_status(redshift, CLUSTER_NAME, 'available'))
    thread.start()

    # wait here for the result to be available before continuing
    while not cluster_event.wait(timeout=5):        
        print('\r{:5}Waited for {} seconds. Redshift Cluster Deletion in-progress...'.format('', progress), end='', flush=True)
    print('\r{:5}Cluster deletion completed. Took {} seconds.'.format('', progress))    

    delete_iam_role(iam, IAM_ROLE_NAME)
    
    print('Please go and manually delete the bucket')
    #print('4. Deleting S3 Bucket')
    #bucket = s3.Bucket(bucket_name)
    
    #bucket.objects.all().delete()    
    #bucket.delete()
    
    print('Done!')   
    
def delete_cluster(redshift, CLUSTER_NAME):
    """
    Summary line. 
    Deletes Redshift Cluster
  
    Parameters: 
    arg1 : Redshift Object
    arg2 : Cluster Name
  
    Returns: 
    None
    """        
    redshift.delete_cluster( ClusterIdentifier=CLUSTER_NAME,  SkipFinalClusterSnapshot=True)    
    
def prettyRedshiftProps(props):
    """
    Summary line. 
    Returns the Redshift Cluster Properties in a dataframe
  
    Parameters: 
    arg1 : Redshift Properties
  
    Returns: 
    dataframe with column key, value
    """        
    
    pd.set_option('display.max_colwidth', -1)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k,v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])

def check_cluster_status(redshift, CLUSTER_NAME, action, status):
    """
    Summary line. 
    Check the cluster status in a loop till it becomes available/none. 
    Once the desired status is set, updates the threading event variable
  
    Parameters: 
    arg1 : Redshift Object
    arg2 : Cluster Name
    arg3 : action which can be (create or delete)
    arg4 : status value to check 
    
    Returns: 
    NONE
    """        
    
    global progress
    global cluster_status

    # wait here for the result to be available before continuing        
    while cluster_status.lower() != status:
        time.sleep(5)
        progress+=5
        if action == 'create':
            myClusterProps = redshift.describe_clusters(ClusterIdentifier=CLUSTER_NAME)['Clusters'][0]
            #print(myClusterProps)
            df = prettyRedshiftProps(myClusterProps)
            #print(df)
            #In keysToShow 2 is ClusterStatus
            cluster_status = df.at[2, 'Value']            
        elif action =='delete':
            myClusterProps = redshift.describe_clusters()
            #print(myClusterProps)
            if len(myClusterProps['Clusters']) == 0 :
                cluster_status = 'none'
            else:
                myClusterProps = redshift.describe_clusters(ClusterIdentifier=CLUSTER_NAME)['Clusters'][0]
                #print(myClusterProps)
                df = prettyRedshiftProps(myClusterProps)
                #print(df)
                #In keysToShow 2 is ClusterStatus
                cluster_status = df.at[2, 'Value']                            

        print('Cluster Status = ',cluster_status)
                
    # when the calculation is done, the result is stored in a global variable
    cluster_event.set()

    # Thats it


def delete_iam_role(iam, IAM_ROLE_NAME):
    """
    Summary line. 
    Delete IAM Role that allows Redshift clusters to call AWS services on your behalf
  
    Parameters: 
    arg1 : IAM Object
    arg2 : IAM Role name
  
    Returns: 
    NONE
    """        
    
    try:
        print('2. Detach Policy')
        iam.detach_role_policy(RoleName=IAM_ROLE_NAME, PolicyArn="arn:aws:iam::957808882659:role/myRedshiftRole25")
        print('3. Delete Role : {}'.format(IAM_ROLE_NAME))
        iam.delete_role(RoleName=IAM_ROLE_NAME)
    except Exception as e:
        print(e)
        
def get_cluster_properties(redshift, CLUSTER_NAME):
    """
    Summary line. 
    Retrieve Redshift clusters properties
  
    Parameters: 
    arg1 : Redshift Object
    arg2 : Cluster Name
  
    Returns: 
    myClusterProps=Cluster Properties, DWH_ENDPOINT=Host URL, DWH_ROLE_ARN=Role Amazon Resource Name
    """        
    
    myClusterProps = redshift.describe_clusters()
    #print(myClusterProps)
    if len(myClusterProps['Clusters']) == 0 :
        #print('Clusters = 0')
        return '', '', ''
    else:
        #print('Clusters != 0')
        myClusterProps = redshift.describe_clusters(ClusterIdentifier=CLUSTER_NAME)['Clusters'][0]
        ENDPOINT = myClusterProps['Endpoint']['Address']
        ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']
        #print("DWH_ENDPOINT :: ", DWH_ENDPOINT)
        #print("DWH_ROLE_ARN :: ", DWH_ROLE_ARN)
        return myClusterProps, ENDPOINT, ROLE_ARN

def main():
    
    initialize()

if __name__ == "__main__":
    main()  