
import pandas as pd
import logging

from dotenv import load_dotenv
import os
from table_manager import TableManager
from database_handler import DatabaseHandler 


# Load environment variables
load_dotenv(override=True)

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


#POSTGRES_USER=admin
#POSTGRES_PASSWORD=admin
#POSTGRES_DB=postgres
#POSTGRES_HOST=localhost
#POSTGRES_PORT=5432

def main():
    
    # Get database credentials from .env file
    DB_CONFIG = {
    "user" : os.getenv("POSTGRES_USER"),
    "password" :os.getenv("POSTGRES_PASSWORD"),
    "dbname" : os.getenv("POSTGRES_DB"),
    "host" : os.getenv("POSTGRES_HOST"),
    "port" : os.getenv("POSTGRES_PORT"),
    }
    
    
    # File paths
    file_paths = {
        "description": "./Datasets/description.csv",
        "travel": "./Datasets/travel.csv",
        "location": "./Datasets/location.csv"
    }

    # Initialize database handler
    db_handler = DatabaseHandler(DB_CONFIG)

    # Create and connect to the database
    cur, conn = db_handler.create_database()

    # Initialize table manager
    table_manager = TableManager(cur, conn)

    # Drop and create tables
    table_manager.drop_tables()
    table_manager.create_tables()

    # Load CSVs into DataFrames
    description = pd.read_csv(file_paths["description"], low_memory=False)
    description.dropna(subset=['index'],inplace=True)
    travel = pd.read_csv(file_paths["travel"])
    location = pd.read_csv(file_paths["location"])

    # Define insert queries
    insert_queries = {
        "description": """
            INSERT INTO description (
                index, id, org_id, url, breed_primary, breed_secondary,
                color_primary, color_secondary, color_tertiary, age, sex,
                size, coat, name, status, posted, contact_city, contact_state,
                contact_zip, contact_country, stateQ
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        "travel": """
            INSERT INTO travel (
                index, id, contact_city, contact_state, found, manual,
                remove, still_there
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        "location": """
            INSERT INTO location (
                index, location, exported, imported, total, inUS
            ) VALUES (%s, %s, %s, %s, %s, %s)
        """
    }

    # Insert data
    table_manager.insert_data("description", insert_queries["description"], description)
    table_manager.insert_data("travel", insert_queries["travel"], travel)
    table_manager.insert_data("location", insert_queries["location"], location)

    # Close the connection
    db_handler.close_connection(conn)


if __name__ == "__main__":
    main()