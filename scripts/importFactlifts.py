import pandas as pd
import os
from sqlalchemy import text
from datetime import datetime
from utils import setupLogger, createNewMembers, setupConnection, replaceAndAppend, fetchExcelFromS3

#create the logger
logging = setupLogger('import-fact-lifts')

#connect to db
engine = setupConnection()
logging.info('Database connection established successfully.')

#fetch excel file from S3
df = fetchExcelFromS3('lifting-data-bucket','userdata/liftingdata/liftingexceldoc_20241125_225903.xlsx','For DB - Lifts')

with engine.connect() as connection:
    try:
        connection.execute(text("SELECT 1"))
        logging.info("Connection successful.")
    except Exception as e:
        logging.error(f"Connection failed: {e}")

#create new members and insert into fact table
with engine.connect() as connection:
    transaction = connection.begin()

    #replace out old data
    replaceAndAppend(connection,'Lift."FactLifts"', df, ["ApplyDate"])
    
    try:
        for index, row in df.iterrows():

            #create fk dictionaries for each column with an fk constraint 
            #these will need to be updated for any new columns with an fk constraint or if the default value changes
            routineFkValues = {'Workout1ID':0, 'Workout2ID':0, 'Workout3ID':0, 'Workout4ID':0, 'Workout5ID':0, 'Workout6ID':0, 'Workout7ID':0}
            workoutFkValues = {'MovementID': 0}

            #create pk dictionaries for each column that has a unique constraint other than the code itself
            workoutPkValues = {'MovementSequence': row['Sequence']}
            logging.info("FK and PK dictionaries created.")

            routineID = createNewMembers(
                connection, 
                'DimRoutines',
                'RoutineCode',
                'RoutineID',
                row['RoutineCode'],
                routineFkValues
            )
            print("Retrieved new routineID")

            workoutID = createNewMembers(
                connection,
                'DimWorkouts',
                'WorkoutCode', 
                'WorkoutID',
                row['WorkoutCode'],
                workoutFkValues,
                workoutPkValues
            )
            print("Retrieved new workoutID")

            #insert into lifts table
            insertFactQuery = text('''
            INSERT INTO lift."FactLifts" ("RoutineID","WorkoutID","MovementID","Reps1","Weight1","Reps2","Weight2","Reps3","Weight3","Reps4","Weight4","IsSuperset","IsSkipped","ApplyDate","Sequence","IsSub","UserID") 
            VALUES (:routineID,:workoutID,:MovementName,:Reps1,:Weight1,:Reps2,:Weight2,:Reps3,:Weight3,:Reps4,:Weight4,:IsSuperset,:IsSkipped,:ApplyDate,:Sequence,:IsSub,:UserID)
            '''
            )
            #correct column data types
            params = {
                'routineID' : routineID,
                'workoutID' :workoutID,
                'MovementName' :str(row['MovementName']) if pd.notnull(row['MovementName']) else '', 
                'Reps1' :int(row['Reps1']) if pd.notna(row['Reps1']) and row['Reps1'] not in ['None', ''] else 0,
                'Weight1' :float(row['Weight1']) if pd.notna(row['Weight1']) and row['Weight1'] not in ['None', ''] else 0,
                'Reps2' :int(row['Reps2']) if pd.notna(row['Reps2']) and row['Reps2'] not in ['None', ''] else 0,
                'Weight2' :float(row['Weight2']) if pd.notna(row['Weight2']) and row['Weight2'] not in ['None', ''] else 0,
                'Reps3' :int(row['Reps3']) if pd.notna(row['Reps3']) and row['Reps3'] not in ['None', ''] else 0,
                'Weight3' :float(row['Weight3']) if pd.notna(row['Weight3']) and row['Weight3'] not in ['None', ''] else 0,
                'Reps4' :int(row['Reps4']) if pd.notna(row['Reps4']) and row['Reps4'] not in ['None', ''] else 0,
                'Weight4' :float(row['Weight4']) if pd.notna(row['Weight4']) and row['Weight4'] not in ['None', ''] else 0,
                'IsSuperset' :bool(row['IsSuperset'] if pd.notna(row['IsSuperset']) and row['IsSuperset'] in [1, 'True'] else False),
                'IsSkipped' :bool(row['IsSkipped'] if pd.notna(row['IsSkipped']) and row['IsSkipped'] in [1, 'True'] else False),
                'ApplyDate' :pd.to_datetime(row['ApplyDate']).date() if pd.notnull(row['ApplyDate']) else datetime(1900, 1, 1).date(),
                'Sequence' :int(row['Sequence']) if row['Sequence'] else 0,
                'IsSub' :bool(row['IsSub'] if pd.notna(row['IsSub']) and row['IsSub'] in [1, 'True'] else False),
                'UserID' :int(row['UserID']) if row['UserID'] else 0,
            }

            logging.info(f"Inserting data for index {index}: {params}")

            connection.execute(insertFactQuery,params)

        transaction.commit()
        print("Transaction committed successfully")

    except Exception as e:
        transaction.rollback()
        logging.error(f"Error during transaction: {e}")
        print("Transaction rolled back due to error")