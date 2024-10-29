import pandas as pd
import os
from sqlalchemy import text
from datetime import datetime
from utils import setupLogger, createNewMembers, setupConnection

#create the logger
logging = setupLogger('import-fact-weights')

#connect to db
engine = setupConnection()
logging.info('Database connection established successfully.')

#load the excel file into data frame + make system agnostic 
baseDir = os.path.dirname(os.path.abspath(__file__))
excelFile = os.path.join(baseDir, '..','data', 'liftingexceldoc.xlsx')
df = pd.read_excel(excelFile, sheet_name= 'Weights')

#test connection
with engine.connect() as connection:
    try:
        connection.execute(text("SELECT 1"))
        logging.info("Connection successful.")
    except Exception as e:
        logging.error(f"Connection failed: {e}")

with engine.connect() as connection:
    transaction = connection.begin()

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
                'Date' :pd.to_datetime(row['Day']).date() if pd.notnull(row['Day']) else datetime(1900, 1, 1).date(),
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
    
