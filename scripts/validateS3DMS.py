import boto3
import pandas as pd
import logging
from utils import setupLogger
import pyarrow
from io import BytesIO
import psycopg2

#create the logger
logging = setupLogger('validate-dms-s3')

def fetchParquetFromS3(bucketName, fileKey):
    '''
    Retrieves lifting excel file from S3 bucket

    Parameters:
        -bucketName (str): bucket name that the file lives in
        -fileKey (str): file key for lifting file

    returns: data frame of excel data
    '''
    try:
        #create an S3 client
        s3Client = boto3.client('s3')

        #grab the object from the bucket
        response = s3Client.get_object(Bucket=bucketName, Key=fileKey)

        body = BytesIO(response['Body'].read())

        df = pd.read_parquet(body, engine='pyarrow')

        return df

    except Exception as e:
        logging.error(f'Error occurred while fetching the excel file from S3: {e}')
        raise e

#update with parquet file you are validating
df = fetchParquetFromS3('lifting-parquet-files','factlifts/parquetFactLifts_20250105_003346.parquet')
    
print(df)