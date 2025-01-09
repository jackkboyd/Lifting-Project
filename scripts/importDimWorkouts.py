import pandas as pd
import os
from sqlalchemy import text
from datetime import datetime
from utils import setupLogger, setupConnection, fetchExcelFromS3

def processDimWorkouts():
    #craete new members not needed - importing into a dimension table with no fk constraints

    #create the logger
    logging = setupLogger('/opt/airflow/logs','import-dim-workouts')

    #connect to db
    engine = setupConnection()
    logging.info('Database connection established successfully.')

    #fetch excel file from S3
    df = fetchExcelFromS3('lifting-data-bucket','For DB - Workouts')

    with engine.connect() as connection:
        transaction = connection.begin()

        try:
            for index, row in df.iterrows():

                #create update statement 

                updateDimQuery = text('''UPDATE lift."DimWorkouts"
                                    SET "WorkoutName" = :WorkoutName,
                                    "MovementName" = :MovementName,
                                    "MovementSequence" = :MovementSequence,
                                    "IsSuperset" = :IsSuperSet,
                                    "StartDate" = :StartDate,
                                    "EndDate" = :EndDate
                                    WHERE "WorkoutCode" = :WorkoutCode
                                    AND "MovementSequence" = :MovementSequence
                '''
                )
                #set parameters for current row
                params = {
                    'WorkoutName': row['WorkoutName'],
                    'MovementName' : row['MovementName'],
                    'MovementSequence' : int(row['MovementSequence']) if pd.notna(row['MovementSequence']) and row['MovementSequence'] not in ['None', ''] else 0,
                    'IsSuperSet' : bool(row['IsSuperset'] if pd.notna(row['IsSuperset']) and row['IsSuperset'] in [1, 'True'] else False),
                    'StartDate' : pd.to_datetime(row['StartDate']).date() if pd.notnull(row['StartDate']) else datetime(1900,1,1).date(),
                    'EndDate' : pd.to_datetime(row['EndDate']).date() if pd.notnull(row['EndDate']) else datetime(1900,1,1).date(),
                    'WorkoutCode' : int(row['WorkoutCode']) if pd.notnull(row['WorkoutCode']) and row['WorkoutCode'] not in ['None',''] else 0
                }

                logging.info(f"Inserting data for index {index}: {params}")

                connection.execute(updateDimQuery,params)

            transaction.commit()
            print("Transaction committed successfully")

        except Exception as e:
            transaction.rollback()
            logging.error(f"Error during transaction: {e}")
            print("Transaction rolled back due to error")

if __name__ == "__main__":
    processDimWorkouts()