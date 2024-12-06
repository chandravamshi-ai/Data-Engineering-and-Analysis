"""Script to load data into Docker Postgres database"""
import os
import pandas as pd
import warnings
import shutil
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Suppress warnings
warnings.filterwarnings('ignore')

# Load environment variables
load_dotenv()

# Database connection string
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://admin:admin@localhost:5432/learndbt")

# Initialize SQLAlchemy engine
engine = create_engine(DATABASE_URL)

# Unpack the data archive
print('Unpacking archive...')
try:
    shutil.unpack_archive('archive.zip')
    print('Archive unpacked successfully.')
except Exception as e:
    print(f"Failed to unpack archive: {e}")
    exit()

# Load data into a DataFrame
try:
    honey_pot_data = pd.read_csv('AWS_Honeypot_marx-geo.csv')
    print('CSV file read successfully.')
except FileNotFoundError:
    print("CSV file not found. Ensure 'AWS_Honeypot_marx-geo.csv' exists in the directory.")
    exit()
except Exception as e:
    print(f"Error reading CSV file: {e}")
    exit()

# Fill missing values
print('Cleaning data...')
cleaned_data = honey_pot_data.copy()

# Fill string-based nulls with 'unknown'
string_columns = ['type', 'country', 'cc', 'locale', 'localeabbr', 'postalcode']
for col in string_columns:
    cleaned_data[col].fillna('unknown', inplace=True)

# Fill numeric nulls with 0
numeric_columns = ['spt', 'dpt']
for col in numeric_columns:
    cleaned_data[col].fillna(0, inplace=True)

# Drop unnecessary columns
if 'Unnamed: 15' in cleaned_data.columns:
    cleaned_data.drop(columns=['Unnamed: 15'], inplace=True)

# Drop rows with any remaining nulls
cleaned_data.dropna(inplace=True)
print('Data cleaning completed.')

# Write the cleaned data to PostgreSQL
print('Writing data to the database...')
try:
    cleaned_data.to_sql(name='hacker_data', con=engine, if_exists='replace', index=False)
    print("Data successfully written to the database table 'hacker_data'.")
except Exception as e:
    print(f"Failed to write data to the database: {e}")
