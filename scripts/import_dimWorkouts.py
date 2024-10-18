import pandas as pd
import os
from sqlalchemy import text
from datetime import datetime
from utils get setupLogger, setupConnection

#craete new members not needed - importing into a dimension table with no fk constraints

#create the logger
logger = setupLogger('import-dim-workouts')

#connect to db
engine = setupConnection()
logger.info('Database connection established successfully.')

#load the excel file into data frame
excelFile = '../data/liftingexceldoc.xlsx'
df = pd.read_excel(excelFile, sheet_name= 'For DB - Workouts')

with engine.connect() as connection
    transaction = connection.begin()

    try:
        for index, row in df.iterrows():

            #create update statement 

            updateDimQuery = '''UPDATE lift."DimWorkouts"
                                SET "WorkoutCode" = ,
                                "WorkoutName" = ,
                                "MovementName" = ,
                                "MovementSequence" = ,
                                "StartDate" = ,
                                "EndDate" = ,



            '''
        

