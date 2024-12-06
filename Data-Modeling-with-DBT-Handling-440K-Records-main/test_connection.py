import os
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv

# Load environment variables from the .env file
load_dotenv()

# Read database credentials from the .env file
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

# Create the connection URL
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Test the connection
try:
    engine = create_engine(DATABASE_URL)
    with engine.connect() as connection:
        print("Connected to the PostgreSQL database successfully!")
except SQLAlchemyError as e:
    print(f"An error occurred: {e}")
