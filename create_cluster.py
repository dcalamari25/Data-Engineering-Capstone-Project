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
    This function starts the create_cluster function. 
  
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
    DB_PORT               = config.get("CLUSTER","DB_PORT")

    IAM_ROLE_NAME      = config.get("CLUSTER", "IAM_ROLE_NAME")
    

    df = pd.DataFrame({"Param":
                    ["CLUSTER_TYPE", "NUM_NODES", "NODE_TYPE", "CLUSTER_NAME", "DB_NAME", "DB_USER", "DB_PASSWORD", "DB_PORT", "IAM_ROLE_NAME"],
                "Value":
                    [CLUSTER_TYPE, NUM_NODES, NODE_TYPE, CLUSTER_NAME, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT, IAM_ROLE_NAME]
                })

    print(df)


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

    
    roleArn = create_iam_role(iam, IAM_ROLE_NAME)
    
    create_cluster(redshift, roleArn, CLUSTER_TYPE, NODE_TYPE, NUM_NODES, DB_NAME, CLUSTER_NAME, DB_USER, DB_PASSWORD)

    #thread = threading.Thread(target=check_cluster_status)
    thread = threading.Thread(target=lambda : check_cluster_status(redshift, CLUSTER_NAME, 'create', 'available'))
    #thread = threading.Thread(target=lambda : check_cluster_status(redshift, CLUSTER_NAME, 'available'))
    thread.start()

    # wait here for the result to be available before continuing
    while not cluster_event.wait(timeout=5):        
        print('\r{:5}Waited for {} seconds. Redshift Cluster Creation in-progress...'.format('', progress), end='', flush=True)
    print('\r{:5}Cluster creation completed. Took {} seconds.'.format('', progress))    
    
    myClusterProps = get_cluster_properties(redshift, CLUSTER_NAME)
    #print(myClusterProps)
    prettyRedshiftProps(myClusterProps[0])
    ENDPOINT = myClusterProps[1]
    ROLE_ARN = myClusterProps[2]
    print('ENDPOINT = {}'.format(ENDPOINT))
    print('ROLE_ARN = {}'.format(ROLE_ARN))
    
    open_ports(ec2, myClusterProps[0], DB_PORT)

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format( ENDPOINT, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT ))
    cur = conn.cursor()

    print('Connected')    

    conn.close()
    
    print('Done!')
    
    
def create_iam_role(iam, IAM_ROLE_NAME):
    """
    Creates IAM Role that allows Redshift clusters to call AWS services on your behalf
  
    Parameters: 
    arg1 : IAM Object
    arg2 : IAM Role name
  
    Returns: 
    NONE
    """        
    
    try:
        print("1.1 Creating a new IAM Role") 
        Role = iam.create_role(
            Path='/',
            RoleName=IAM_ROLE_NAME,
            Description = "Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                'Effect': 'Allow',
                'Principal': {'Service': 'redshift.amazonaws.com'}}],
                'Version': '2012-10-17'})
        )    
    except Exception as e:
        print(e)

    print("1.2 Attaching Policy")
    iam.attach_role_policy(RoleName=IAM_ROLE_NAME,
                        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                        )['ResponseMetadata']['HTTPStatusCode']

    print("1.3 Get the IAM role ARN")
    roleArn = iam.get_role(RoleName=IAM_ROLE_NAME)['Role']['Arn']

    print('{:5} ARN : {}'.format('',roleArn))
    return roleArn


def create_cluster(redshift, roleArn, CLUSTER_TYPE, NODE_TYPE, NUM_NODES, DB_NAME, CLUSTER_NAME, DB_USER, DB_PASSWORD):
    """
    Creates Redshift Cluster
  
    Parameters: 
    arg1 : Redshift Object
    arg2 : Cluster Name
  
    Returns: 
    None
    """        
    
    print('1.4 Starting Redshift Cluster Creation')
    try:
        response = redshift.create_cluster(        
            #HW
            ClusterType=CLUSTER_TYPE,
            NodeType=NODE_TYPE,
            NumberOfNodes=int(NUM_NODES),

            #Identifiers & Credentials
            DBName=DB_NAME,
            ClusterIdentifier=CLUSTER_NAME,
            MasterUsername=DB_USER,
            MasterUserPassword=DB_PASSWORD,
            
            #Roles (for s3 access)
            IamRoles=[roleArn]  
        )
    except Exception as e:
        print(e)

def prettyRedshiftProps(props):
    """
    Returns the Redshift Cluster Properties in a dataframe
  
    Parameters: 
    arg1 : Redshift Properties
  
    Returns: 
    dataframe with column key, value
    """        
    
    pd.set_option('display.max_colwidth', -1)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    #print(props)
    x = [(k, v) for k,v in props.items() if k in keysToShow]
    '''
    #(OR) Below is longer version above is shorter version
    xx = []
    for k in props:
        if k in keysToShow:
            v = props.get(k)
            xx.append((k,v))
            print('{} : {}'.format(k, v))    
    print('XX = ',xx)
    '''
    #print('X = ',x)
    return pd.DataFrame(data=x, columns=["Key", "Value"])

def check_cluster_status(redshift, CLUSTER_NAME, action, status):
    """
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
      
    myClusterProps = redshift.describe_clusters(ClusterIdentifier=CLUSTER_NAME)['Clusters'][0]
    ENDPOINT = myClusterProps['Endpoint']['Address']
    ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']
    print("ENDPOINT :: ", ENDPOINT)
    print("ROLE_ARN :: ", ROLE_ARN)
    return myClusterProps, ENDPOINT, ROLE_ARN

def open_ports(ec2, myClusterProps, DB_PORT):
    """
    Summary line. 
    Update clusters security group to allow access through redshift port
  
    Parameters: 
    arg1 : ec2 Object
    arg2 : Cluster Properties
    arg3 : Redshift Port
  
    Returns: 
    NONE
    """        

    try:
        vpc = ec2.Vpc(id=myClusterProps['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        print(defaultSg)
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(DB_PORT),
            ToPort=int(DB_PORT)
        )
    except Exception as e:
        print(e)


def main():
    
    initialize()

if __name__ == "__main__":
    main()
    