
#import librariesa
import pandas as pd # For data Extraction and Manipulation
import psycopg2 # For connecting Python to postgreSQL database
from sqlalchemy import create_engine # To efficiently manage and reuse the database connections


# Get the data path and store it in data
data = pd.read_csv(r"C:\Users\Edu\Downloads\ETL Pipeline\commerce_data.csv")

# view the top five rows
data.head()

# View the bottom five rows
data.tail()

# Data Transformation
data.duplicated().sum() # check for duplicate data

data.drop_duplicates(keep='first' , inplace=True) # remove duplicates if any and keep the first ones

# check missing data
data.isnull().sum()

# database credentioals
username = 'postgres'
password = 'postgress123'
host = 'localhost'
port = 5432
db_name = 'postgres'

# create connection
engine = create_engine(f'postgresql://{username}:{password}@{host}:{port}/{db_name}')

# load database table
data.to_sql('commerce_Data', engine, if_exists='replace', index=False)

# close the connection
engine.dispose()
