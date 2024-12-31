import boto3
from sqlalchemy import text
import pandas as pd
import pyarrow
from utils import setupConnection, setupLogger, uploadParquetfile

#setup logging
logging = setupLogger('factweights-parquet-to-S3')

#connect to db
engine = setupConnection()
logging.info('Database connection established successfully.')

#test connection 
with engine.connect() as connection:
    try:
        connection.execute(text("SELECT 1"))
        logging.info("Connection successful.")
    except Exception as e:
        logging.error(f"Connection failed: {e}")

#pull fact lifts into df
with engine.connect() as connection:
    try:
        #pull data to data frame
        selectQuery = 'select * from lift."FactWeights"'
        df = pd.read_sql_query(selectQuery, connection)
        logging.info('Pulled postgres data to dataframe.')

        #convert df to parquet file and upload to S3
        uploadParquetfile(df, 'parquetFactWeights', 'lifting-parquet-files')

    except Exception as e:
        logging.error(f"Error during transaction: {e}")
        print("Error during transaction")

        