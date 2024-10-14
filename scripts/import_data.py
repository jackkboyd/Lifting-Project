import pandas as pd
import os
import psycopg2
from sqlalchemy import create_engine, text
from datetime import datetime
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("../logs/app.log")
    ])

#retrieve environment variables
USER = os.getenv('DATABASE_USER')
PASSWORD = os.getenv('DATABASE_PASSWORD')
ENDPOINT = os.getenv("DATABASE_ENDPOINT")
DATABASE = os.getenv('DATABASE_NAME')
PORT = os.getenv('DATABASE_PORT')

#load the excel file into data frame
excelFile = '../data/liftingexceldoc.xlsx'
df = pd.read_excel(excelFile, sheet_name= 'For DB - Lifts')

#create the connection engine
engine = create_engine(f'postgresql+psycopg2://{USER}:{PASSWORD}@{ENDPOINT}:{PORT}/{DATABASE}')

with engine.connect() as connection:
    try:
        connection.execute(text("SELECT 1"))
        logging.info("Connection successful.")
    except Exception as e:
        logging.error(f"Connection failed: {e}")

def createNewMembers(connection, tableName, codeColumn, IDColumn, codeValue, fk_constraints={}):
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

    Returns:
        -Associated ID of the dimension member
    """
    #create a dictionary with columns / values for all columns with an fk constraint
    #if no ID code is passed through ID defaults to 0
    fk_values = {col: fk_constraints.get(col,0) for col in fk_constraints}

    #create list of column names 
    columns = [codeColumn] + list(fk_values.keys())
    placeholders = [':codeValue'] + [f':{col}' for col in fk_values.keys()]

    checkQuery = text(f'Select "{IDColumn}" from lift."{tableName}" where "{codeColumn}" = :codeValue')
    result = connection.execute(checkQuery, {'codeValue': codeValue}).fetchone()

    if result:
            print("New member exists in the dim")
            return result[0]
    else:
        insertDimQuery = text(f'INSERT INTO lift."{tableName}" ({", ".join(f'"{col}"' for col in columns)}) VALUES ({", ".join(placeholders)}) RETURNING "{IDColumn}"')
        params = {'codeValue': codeValue, **fk_values}
        newID = connection.execute(insertDimQuery, params).fetchone()[0]   
        print("New member created in the dim")
    return newID

#create new members and insert into fact table
with engine.connect() as connection:
    transaction = connection.begin()
    try:
        for index, row in df.iterrows():

            #create fk dictionaries for each column with an fk constraint 
            #these will need to be updated for any new columns with an fk constraint or if the default value changes
            routineFkValues = {'Workout1ID':0, 'Workout2ID':0, 'Workout3ID':0, 'Workout4ID':0, 'Workout5ID':0, 'Workout6ID':0, 'Workout7ID':0}
            workoutFkValues = {'MovementID': 0}

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
                workoutFkValues
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
                'IsSuperset' :bool(row['IsSuperset'] if row['IsSuperset'] in ['True','1'] else False),
                'IsSkipped' :bool(row['IsSkipped'] if row['IsSkipped'] in ['True','1'] else False),
                'ApplyDate' :pd.to_datetime(row['ApplyDate']).date() if pd.notnull(row['ApplyDate']) else datetime(1900, 1, 1).date(),
                'Sequence' :int(row['Sequence']) if row['Sequence'] else 0,
                'IsSub' :bool(row['IsSub'] if row['IsSub'] in ['IsSub','1'] else False),
                'UserID' :str(row['UserID'] if row['UserID'] in ['NaN',''] else '0')
            }

            logging.info(f"Inserting data for index {index}: {params}")

            connection.execute(insertFactQuery,params)

        transaction.commit()
        print("Transaction committed successfully")

    except Exception as e:
        transaction.rollback()
        logging.error(f"Error during transaction: {e}")
        print("Transaction rolled back due to error")