import pandas as pd
import os
import psycopg2
from sqlalchemy import create_engine, text

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

def createNewMembers(connection, tableName, codeColumn, IDColumn, codeValue,):
    """
    function to create new members and retrieve ID 
    Parameters:
        -connection - the db connection
        -tableName - dimension table we are inserting to / referencing
        -codeColumn - column in the dimension table we are inserting / referncing
        -IDColumn - associated Id of the dimension member
        -codeValue - the actual code to insert / refer to 

    Returns:
        -Associated ID of the dimension member
    """

    checkQuery = text(f'Select "{IDColumn}" from lift."{tableName}" where "{codeColumn}" = :codeValue')
    result = connection.execute(checkQuery, {'codeValue': codeValue}).fetchone()

    if result:
            return result['{IDColumn}']
    else:
        insertDimQuery = text(f'INSERT INTO lift."{tableName}" ("{codeColumn}") VALUES (:codeValue) RETURNING "{IDColumn}"')
        newID = connection.execute(insertDimQuery, {'codeValue': codeValue}).fetchone()[0]
    return newID

#create new members and insert into fact table
with engine.connect() as connection:
    for index, row in df.iterrows():
        routineID = createNewMembers(connection, 'DimRoutines', 'RoutineCode', 'RoutineID', row['RoutineCode'])
        workoutID = createNewMembers(connection, 'DimWorkouts', 'WorkoutCode', 'WorkoutID', row['WorkoutCode'])

        #insert into lifts table
        insertFactQuery = '''
        INSERT INTO lifts."FactLifts" ("RoutineID","WorkoutID","MovementID","Reps1","Weight1","Reps2","Weight2","Reps3","Weight3","Reps4","Weight4","IsSuperset","IsSkipped","ApplyDate","Sequence","IsSub") 
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        '''
        connection.execute(insertFactQuery,(routineID,workoutID,row['MovementName'],row['Reps1'],row['Weight1'],row['Reps2'],row['Weight2'],row['Reps3'],row['Weight3'],row['Reps4'],row['Weight4'],row['IsSuperset'],row['IsSkipped'],row['ApplyDate'],row['Sequence'],row['IsSub']))

print("Data imported successfully!")

