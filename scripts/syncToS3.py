import os
import boto3
from datetime import datetime
from botocore.exceptions import ClientError
import json

def get_secret():

    secret_name = "lift-credentials"
    region_name = "us-east-2"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e
    
    secret = get_secret_value_response['SecretString']
    return json.loads(secret)


#get aws credentials from secrets manager
creds = get_secret()

#extract keys from secret
awsAccessKey = creds['aws_access_key']
awsSecretKey = creds['aws_secret_key']

#create an S3 client
s3Client = boto3.client(
    's3',
    aws_access_key_id = awsAccessKey,
    aws_secret_access_key = awsSecretKey,
    region_name='us-east-2'
    )

#load the excel file into data frame + make system agnostic 
baseDir = os.path.dirname(os.path.abspath(__file__))
excelFile = os.path.join(baseDir, '..','data', 'liftingexceldoc.xlsx')

#set the s3 key + concat timestamp
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
s3Key = f'userdata/liftingdata/liftingexceldoc_{timestamp}.xlsx'
s3Client.upload_file(excelFile,'lifting-data-bucket', s3Key)
