import os
import logging
from sqlalchemy import create_engine, text
import pandas as pd
import boto3
from botocore.exceptions import ClientError
import json
from io import BytesIO
import io
import pyarrow
from datetime import datetime
import psycopg2

#1 set up logging
def setupLogger (log_file_path, log_file_name = 'app'):
    log_path = f"{log_file_path}{log_file_name}.log"

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_path)
        ]
    )
    return logging.getLogger()

# Use this code snippet in your app.
# If you need more information about configurations
# or implementing the sample code, visit the AWS docs:
# https://aws.amazon.com/developer/language/python/

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

#3 create engine connection
def setupConnection ():
    creds = get_secret()

    USER = creds["USER"]
    PASSWORD = creds["PASSWORD"]
    ENDPOINT = creds["ENDPOINT"]
    DATABASE = creds["DATABASE"]
    PORT = creds["PORT"]

    engine = create_engine(f'postgresql+psycopg2://{USER}:{PASSWORD}@{ENDPOINT}:{PORT}/{DATABASE}')
    return engine

#4 create new members script
def createNewMembers(connection, tableName, codeColumn, IDColumn, codeValue, fk_constraints={}, uniqueCodes={}):
    """
    function to create new members and retrieve ID 
    Parameters:
        -connection - the db connection
        -tableName (str) - dimension table we are inserting to / referencing
        -codeColumn (str) - column in the dimension table we are inserting / referncing
        -IDColumn (str) - associated Id of the dimension member
        -codeValue (str) - the actual code to insert / refer to 
        -fk_constraints (dict) - fields on dimensions we are creating new members for that have fk constraints
            -If no fk value is provided it defaults to 0
        -otherCodes (dict) - other colummns in the dimension table that together create the PK / a Unique constraint

    Returns:
        -Associated ID of the dimension member
    """
    #create a dictionary with columns / values for all columns with an fk constraint
    #if no ID code is passed through ID defaults to 0
    fk_values = {col: fk_constraints.get(col,0) for col in fk_constraints}

    #create a dictionary with columns / values for all PK / Unique columns other than the code itself
    pk_values = {col: uniqueCodes.get(col,0) for col in uniqueCodes}

    #create list of column names 
    columns = [codeColumn] + list(fk_values.keys()) + list(pk_values.keys())
    placeholders = [':codeValue'] + [f':{col}' for col in fk_values.keys()] + [f':{col}' for col in pk_values.keys()]

    checkQuery = f'Select "{IDColumn}" from lift."{tableName}" where "{codeColumn}" = :codeValue'

    #dynamically construct check query if unique contraints are provided
    if uniqueCodes:
         
        #create list of unique constraints
        uniqueConditions = [(f'"{pk}" = :{pk}') for pk in pk_values.keys()]

        #join ands for unique constraints 
        uniqueConditionsClause = 'AND '.join(uniqueConditions)

        #join uniqueConditions to checkQuery
        checkQuery += f' AND {uniqueConditionsClause}'
    
    checkQuery = text(checkQuery)

    params = {'codeValue': codeValue, **pk_values}
    result = connection.execute(checkQuery, params).fetchone()

    if result:
            print("New member exists in the dim")
            return result[0]
    else:
        insertDimQuery = text(f'INSERT INTO lift."{tableName}" ({", ".join(f'"{col}"' for col in columns)}) VALUES ({", ".join(placeholders)}) RETURNING "{IDColumn}"')
        params = {'codeValue': codeValue, **fk_values, **pk_values}
        newID = connection.execute(insertDimQuery, params).fetchone()[0]   
        print("New member created in the dim")
    return newID

#create reaplce and append logic
def replaceAndAppend(connection, tableName, df, replaceKeys):
    """

    function to delete out data from table we are inserting into where replace key values match replace key values within df
    Parameters:
        -connection - the db connection
        -tableName (str) - fact table we a replacing out data on
        -replaceKeys (list) - columns that, when matched by df, corresponding records are deleted and replaced by new data in df

    """
    #create a df with all replace key values
    replaceKeyDF = df[replaceKeys].drop_duplicates()

    #start initial delete query
    deleteQuery = f'DELETE FROM {tableName} WHERE '

    #Intialize empty list to hold formatted tuples
    tuples = []

    #Iterate through each row in the df
    for index, row in replaceKeyDF.iterrows():
        #interate through each column in the df
        values = []
        for key in replaceKeys:
            value = row[key]
            #reformat any timestamps
            if isinstance(value, pd.Timestamp):
                formattedValue = (f"'{value.strftime('%Y-%m-%d')}'")
            else:
                formattedValue = f"'{value}'" 
            
            values.append(formattedValue)

        tuples.append(f'({", ".join(values)})')

    columnNames = [f'"{col}"' for col in replaceKeyDF.columns]

    columnNamesFormatted = ", ".join(columnNames)

    whereFormatted = f"({columnNamesFormatted}) IN ({", ".join(tuples)})"

    deleteQuery = text(deleteQuery + whereFormatted)

    connection.execute(deleteQuery)

def fetchExcelFromS3(bucketName, fileKey, sheetName):
    '''
    Retrieves lifting excel file from S3 bucket

    Parameters:
        -bucketName (str): bucket name that the file lives in
        -fileKey (str): file key for lifting file
        -sheetName (str): sheet name of the excel doc to pull data from

    returns: data frame of excel data
    '''
    try:
        #create an S3 client
        s3Client = boto3.client('s3')

        #grab the object from the bucket
        response = s3Client.get_object(Bucket=bucketName, Key=fileKey)

        #read excel file into df using bytesio
        fileData = response['Body'].read()
        excelFile = BytesIO(fileData)

        df = pd.read_excel(excelFile, sheetName)

        logging.info("Excel file successfully loaded from S3")
        return df

    except Exception as e:
        logging.error(f'Error occurred while fetching the excel file from S3: {e}')
        raise e
    
def uploadParquetfile(df, folder, file_name, bucket, object_name=None):
    """Converts a dataframe to a parquet file and uploads to an S3 bucket

    :param df: data frame to be converted to parquet and uploaded
    :param folder: file path in S3 bucket
    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """
    #append timestamp and file type to filename
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    file_name = f'{folder}{file_name}_{timestamp}.parquet'
    logging.info(f'created new file name {file_name}')

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name
    logging.info('set object name')

    #convert datframe ot parquet leveraging in memory buffer
    try:
        buffer = io.BytesIO()
        df.to_parquet(buffer, engine='pyarrow', index=False)
        buffer.seek(0)
        logging.info('converted to parquet in memory')
    except Exception as e:
        logging.error(f'Error converting to parquet: {e}')
        return False
    
    # Upload the file
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_fileobj(buffer, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True
   










