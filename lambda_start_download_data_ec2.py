import json
import boto3
import os


INSTANCE_ID = os.environ['INSTANCE_ID']  # from aws environment variables

    
def lambda_handler(event, context):
    client = boto3.client('ec2', region_name='eu-central-1')
    response = client.start_instances(InstanceIds=[INSTANCE_ID])
    status_code = response['ResponseMetadata']['HTTPStatusCode']
    if status_code != 200:
        raise Exception('Response status code != 200. Response:', response)
    return {
        'statusCode': status_code,
        'body': response
    }
