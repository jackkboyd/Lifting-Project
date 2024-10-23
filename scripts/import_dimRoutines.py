import pandas as pd
import os
from sqlalchemy import text
from datetime import datetime
from utils import setupLogger, createNewMembers, setupConnection

#create the logger
logging = setupLogger('import-fact-lifts')

#connect to db
engine = setupConnection()
logging.info('Database connection established successfully.')

#load the excel file into data frame + make system agnostic 
baseDir = os.path.dirname(os.path.abspath(__file__))
excelFile = os.path.join(baseDir, '..','data', 'liftingexceldoc.xlsx')
df = pd.read_excel(excelFile, sheet_name= 'For DB - Routines')

#create new members and retrieve correct IDs 
with engine.connect() as connection:
    transaction = connection.begin()

    try:
        for index, row in df.iterrows():

            #set fk values 
            workoutFkValues: {'MovementID':0} # type: ignore
            
            #retrieve IDs and create new members
            workout1ID = createNewMembers(
                connection, 
                'DimWorkouts',
                'Workout1Code',
                'Workout1ID',
                row['Workout1Code'],
                workoutFkValues
            )
            workout2ID = createNewMembers(
                connection, 
                'DimWorkouts',
                'Workout2Code',
                'Workout2ID',
                row['Workout2Code'],
                workoutFkValues
            )
            workout3ID = createNewMembers(
                connection, 
                'DimWorkouts',
                'Workout3Code',
                'Workout3ID',
                row['Workout3Code'],
                workoutFkValues
            )
            workout4ID = createNewMembers(
                connection, 
                'DimWorkouts',
                'Workout4Code',
                'Workout4ID',
                row['Workout4Code'],
                workoutFkValues
            )
            workout5ID = createNewMembers(
                connection, 
                'DimWorkouts',
                'Workout5Code',
                'Workout5ID',
                row['Workout5Code'],
                workoutFkValues
            )
            workout6ID = createNewMembers(
                connection, 
                'DimWorkouts',
                'Workout6Code',
                'Workout6ID',
                row['Workout6Code'],
                workoutFkValues
            )
            workout7ID = createNewMembers(
                connection, 
                'DimWorkouts',
                'Workout7Code',
                'Workout7ID',
                row['Workout7Code'],
                workoutFkValues
            )
             
            #create update statement
            updateDimQuery = text('''UPDATE lift."DimRoutines" 
                                SET "RoutineName" = ,
                                "Workout1ID" = :workout1ID,
                                "Workout2ID" = :workout2ID,
                                "Workout3ID" = :workout3ID,
                                "Workout4ID" = :workout4ID,
                                "Workout5ID" = :workout5ID,
                                "Workout6ID" = :workout6ID,
                                "Workout7ID" = :workout7ID,
                                "StartDate" = :startdate,
                                "EndDate" = :enddate
                                WHERE "RoutineCode" = row['RoutineCode']
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
                'enddate': pd.to_datetime(row['EndDate']).date() if pd.notnull(row['EndDate']) else datetime(1900,1,1).date()
            }

            logging.info(f"Inserting data for index {index}: {params}")
             
            connection.execute(updateDimQuery,params)

        transaction.commit()
        print("Transaction committed successfully")
    
    except Exception as e:
        transaction.rollback()
        logging.error(f"Error during transaction: {e}")
        print("Transaction rolled back due to error")