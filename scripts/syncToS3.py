import os
import boto3
from utils import setupLogger, get_secret
from datetime import datetime

logging = setupLogger('sync-to-S3')

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
