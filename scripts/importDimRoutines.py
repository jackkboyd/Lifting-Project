import pandas as pd
import os
from sqlalchemy import text
from datetime import datetime
from utils import setupLogger, createNewMembers, setupConnection, fetchExcelFromS3

#create the logger
logging = setupLogger('import-dim-routines')

#connect to db
engine = setupConnection()
logging.info('Database connection established successfully.')

#fetch excel file from S3
df = fetchExcelFromS3('lifting-data-bucket','userdata/liftingdata/liftingexceldoc_20241125_225903.xlsx','For DB - Routines')

#create new members and retrieve correct IDs 
with engine.connect() as connection:
    transaction = connection.begin()

    try:
        for index, row in df.iterrows():

            #set fk values 
            workoutFkValues = {'MovementID': 0}
            logging.info('FK Values Set.')
            
            #retrieve IDs and create new members
            workout1ID = createNewMembers(
                connection, 
                'DimWorkouts',
                'WorkoutCode',
                'WorkoutID',
                row['Workout1Code'],
                workoutFkValues
            )
            workout2ID = createNewMembers(
                connection, 
                'DimWorkouts',
                'WorkoutCode',
                'WorkoutID',
                row['Workout2Code'],
                workoutFkValues
            )
            workout3ID = createNewMembers(
                connection, 
                'DimWorkouts',
                'WorkoutCode',
                'WorkoutID',
                row['Workout3Code'],
                workoutFkValues
            )
            workout4ID = createNewMembers(
                connection, 
                'DimWorkouts',
                'WorkoutCode',
                'WorkoutID',
                row['Workout4Code'],
                workoutFkValues
            )
            workout5ID = createNewMembers(
                connection, 
                'DimWorkouts',
                'WorkoutCode',
                'WorkoutID',
                row['Workout5Code'],
                workoutFkValues
            )
            workout6ID = createNewMembers(
                connection, 
                'DimWorkouts',
                'WorkoutCode',
                'WorkoutID',
                row['Workout6Code'],
                workoutFkValues
            )
            workout7ID = createNewMembers(
                connection, 
                'DimWorkouts',
                'WorkoutCode',
                'WorkoutID',
                row['Workout7Code'],
                workoutFkValues
            )
            logging.info('IDs created / retrieved.')
            #create update statement
            updateDimQuery = text('''UPDATE lift."DimRoutines" 
                                SET "RoutineName" = :routineName,
                                "Workout1ID" = :workout1ID,
                                "Workout2ID" = :workout2ID,
                                "Workout3ID" = :workout3ID,
                                "Workout4ID" = :workout4ID,
                                "Workout5ID" = :workout5ID,
                                "Workout6ID" = :workout6ID,
                                "Workout7ID" = :workout7ID,
                                "StartDate" = :startdate,
                                "EndDate" = :enddate
                                WHERE "RoutineCode" = :routineCode
            '''
            )
            
            #set paramters for current row
            params = {
                'workout1ID':workout1ID,
                'workout2ID':workout2ID,
                'workout3ID':workout3ID,
                'workout4ID':workout4ID,
                'workout5ID':workout5ID,
                'workout6ID':workout6ID,
                'workout7ID':workout7ID,
                'routineName': str(row['RoutineName']) if pd.notnull(row['RoutineName']) else '',
                'startdate': pd.to_datetime(row['StartDate']).date() if pd.notnull(row['StartDate']) else datetime(1900,1,1).date(),
                'enddate': pd.to_datetime(row['EndDate']).date() if pd.notnull(row['EndDate']) else datetime(1900,1,1).date(),
                'routineCode' :int(row['RoutineCode']) if pd.notnull(row['RoutineCode']) and row['RoutineCode'] not in ['None',''] else 0
            }

            logging.info(f"Inserting data for index {index}: {params}")
             
            connection.execute(updateDimQuery,params)

        transaction.commit()
        print("Transaction committed successfully")
    
    except Exception as e:
        transaction.rollback()
        logging.error(f"Error during transaction: {e}")
        print("Transaction rolled back due to error")