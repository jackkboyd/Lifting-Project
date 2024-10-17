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
df = pd.read_excel(excelFile, sheet_name= 'For DB - Routines')

#create the connection engine
engine = create_engine(f'postgresql+psycopg2://{USER}:{PASSWORD}@{ENDPOINT}:{PORT}/{DATABASE}')

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