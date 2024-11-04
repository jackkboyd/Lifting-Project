import os
import logging
import psycopg
from sqlalchemy import create_engine, text
import pandas as pd

#1 set up logging
def setupLogger (log_file_name = 'app'):
    log_path = f"../logs/{log_file_name}.log"

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_path)
        ]
    )
    return logging.getLogger()

#2 set up db connection
def getDBCreds ():
    USER = os.getenv('DATABASE_USER')
    PASSWORD = os.getenv('DATABASE_PASSWORD')
    ENDPOINT = os.getenv('DATABASE_ENDPOINT')
    DATABASE = os.getenv('DATABASE_NAME')
    PORT = os.getenv('DATABASE_PORT')

    if not all([USER, PASSWORD, ENDPOINT, DATABASE, PORT]):
        raise ValueError("Missing one or more database environment variables.")

    return USER, PASSWORD, ENDPOINT, DATABASE, PORT

#3 create engine connection
def setupConnection ():
    USER, PASSWORD, ENDPOINT, DATABASE, PORT = getDBCreds()
    engine = create_engine(f'postgresql+psycopg://{USER}:{PASSWORD}@{ENDPOINT}:{PORT}/{DATABASE}')
    return engine

#4 create new members script
def createNewMembers(connection, tableName, codeColumn, IDColumn, codeValue, fk_constraints={}, uniqueCodes={}):
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
        -otherCodes (dict) - other colummns in the dimension table that together create the PK / a Unique constraint

    Returns:
        -Associated ID of the dimension member
    """
    #create a dictionary with columns / values for all columns with an fk constraint
    #if no ID code is passed through ID defaults to 0
    fk_values = {col: fk_constraints.get(col,0) for col in fk_constraints}

    #create a dictionary with columns / values for all PK / Unique columns other than the code itself
    pk_values = {col: uniqueCodes.get(col,0) for col in uniqueCodes}

    #create list of column names 
    columns = [codeColumn] + list(fk_values.keys()) + list(pk_values.keys())
    placeholders = [':codeValue'] + [f':{col}' for col in fk_values.keys()] + [f':{col}' for col in pk_values.keys()]

    checkQuery = f'Select "{IDColumn}" from lift."{tableName}" where "{codeColumn}" = :codeValue'

    #dynamically construct check query if unique contraints are provided
    if uniqueCodes:
         
        #create list of unique constraints
        uniqueConditions = [(f'"{pk}" = :{pk}') for pk in pk_values.keys()]

        #join ands for unique constraints 
        uniqueConditionsClause = 'AND '.join(uniqueConditions)

        #join uniqueConditions to checkQuery
        checkQuery += f' AND {uniqueConditionsClause}'
    
    checkQuery = text(checkQuery)

    params = {'codeValue': codeValue, **pk_values}
    result = connection.execute(checkQuery, params).fetchone()

    if result:
            print("New member exists in the dim")
            return result[0]
    else:
        insertDimQuery = text(f'INSERT INTO lift."{tableName}" ({", ".join(f'"{col}"' for col in columns)}) VALUES ({", ".join(placeholders)}) RETURNING "{IDColumn}"')
        params = {'codeValue': codeValue, **fk_values, **pk_values}
        newID = connection.execute(insertDimQuery, params).fetchone()[0]   
        print("New member created in the dim")
    return newID

#create reaplce and append logic
def replaceAndAppend(connection, tableName, df, replaceKeys):
    """

    function to delete out data from table we are inserting into where replace key values match replace key values within df
    Parameters:
        -connection - the db connection
        -tableName (str) - fact table we a replacing out data on
        -replaceKeys (list) - columns that, when matched by df, corresponding records are deleted and replaced by new data in df

    """
    #create a df with all replace key values
    replaceKeyDF = df[replaceKeys].drop_duplicates()

    #start initial delete query
    deleteQuery = f'DELETE FROM {tableName} WHERE '

    #Intialize empty list to hold formatted tuples
    tuples = []

    #Iterate through each row in the df
    for index, row in replaceKeyDF.iterrows():
        #interate through each column in the df
        values = []
        for key in replaceKeys:
            value = row[key]
            #reformat any timestamps
            if isinstance(value, pd.Timestamp):
                formattedValue = (f"'{value.strftime('%Y-%m-%d')}'")
            else:
                formattedValue = f"'{value}'" 
            
            values.append(formattedValue)

        tuples.append(f'({", ".join(values)})')

    columnNames = [f'"{col}"' for col in replaceKeyDF.columns]

    columnNamesFormatted = ", ".join(columnNames)

    whereFormatted = f"({columnNamesFormatted}) IN ({", ".join(tuples)})"

    deleteQuery = text(deleteQuery + whereFormatted)

    connection.execute(deleteQuery)







