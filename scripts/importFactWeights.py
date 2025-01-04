import pandas as pd
import os
import boto3
from sqlalchemy import text
from datetime import datetime
from utils import setupLogger, createNewMembers, setupConnection, replaceAndAppend, fetchExcelFromS3

def processFactWeights():
    #create the logger
    logging = setupLogger('/opt/airflow/logs','import-fact-weights')

    #connect to db
    engine = setupConnection()
    logging.info('Database connection established successfully.')

    #fetch excel file from S3
    df = fetchExcelFromS3('lifting-data-bucket','userdata/liftingdata/liftingexceldoc_20241125_225903.xlsx','Weights')

    #test connection
    with engine.connect() as connection:
        try:
            connection.execute(text("SELECT 1"))
            logging.info("Connection successful.")
        except Exception as e:
            logging.error(f"Connection failed: {e}")

    with engine.connect() as connection:
        transaction = connection.begin()

        #replace out old data
        replaceAndAppend(connection,'Lift."FactWeights"', df, ["Date"])

        try:
            for index, row in df.iterrows():

                #retrieve IDs
                userID = createNewMembers(
                    connection,
                    'DimUsers',
                    'UserCode',
                    'UserID',
                    row['UserCode']
                )
                
                logging.info('IDs created / retrieved.')

                #create update statement
                updateFactQuery = text('''INSERT INTO lift."FactWeights"
                                    ("UserID","Date","Weight")
                                    VALUES
                                    (:UserID, :Date, :Weight)
                                    '''
                )

                #set params
                params = {
                    'UserID' :userID,
                    'Date' :pd.to_datetime(row['Date']).date() if pd.notnull(row['Date']) else datetime(1900, 1, 1).date(),
                    'Weight' :float(row['Weight']) if pd.notna(row['Weight']) and row['Weight'] not in ['None', ''] else 0
                }

                logging.info(f"Inserting data for index {index}: {params}")

                connection.execute(updateFactQuery, params)

            transaction.commit()
            print("Transaction committed successfully")
        
        except Exception as e:
            transaction.rollback()
            logging.error(f'Error during transaction {e}')
            print("Transaction rolled back due to error")

if __name__ == "__main__":
    processFactWeights()