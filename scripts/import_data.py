import pandas as pd
import psycopg2
from sqlalchemy import create_engine

#load the excel file

excelFile = 'data/liftingexceldoc.xlsx'
df = pd.read_excel(excelFile, sheet_name= 'For DB - Lifts')

print(df.head())
