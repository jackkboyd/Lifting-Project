import os
import logging
import psycopg2
from sqlalchemy import create_engine, text

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
    engine = create_engine(f'postgresql+psycopg2://{USER}:{PASSWORD}@{ENDPOINT}:{PORT}/{DATABASE}')
    return engine

#4 create new members script
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
