import pandas as pd
import os
import psycopg2
from sqlalchemy import create_engine

#retrieve environment variables
USER = os.getenv('DATABASE_USER')
PASSWORD = os.getenv('DATABASE_PASSWORD')
ENDPOINT = os.getenv("DATABASE_ENDPOINT")
DATABASE = os.getenv('DATABASE_NAME')
PORT = os.getenv('DATABASE_PORT')

#load the excel file
excelFile = 'data/liftingexceldoc.xlsx'

#create the connection engine
engine = create_engine(f'postgresql+psycopg2://{USER}:{PASSWORD}@{ENDPOINT}:{PORT}/{DATABASE}')

df = pd.read_excel(excelFile, sheet_name= 'For DB - Lifts')
print(df)



