import logging
import pandas as pd

class TableManager:
    def __init__(self, cur, conn):
        self.cur = cur
        self.conn = conn

    def create_tables(self):
        """Create required tables."""
        table_queries = {
            "description": """
                CREATE TABLE IF NOT EXISTS description (
                    index INT PRIMARY KEY, 
                    id INT, 
                    org_id VARCHAR, 
                    url VARCHAR, 
                    breed_primary VARCHAR, 
                    breed_secondary VARCHAR, 
                    color_primary VARCHAR, 
                    color_secondary VARCHAR, 
                    color_tertiary VARCHAR, 
                    age VARCHAR, 
                    sex VARCHAR, 
                    size VARCHAR, 
                    coat VARCHAR, 
                    name VARCHAR, 
                    status VARCHAR, 
                    posted VARCHAR, 
                    contact_city VARCHAR, 
                    contact_state VARCHAR, 
                    contact_zip VARCHAR, 
                    contact_country VARCHAR, 
                    stateQ VARCHAR
                )
            """,
            "travel": """
                CREATE TABLE IF NOT EXISTS travel (
                    index INT PRIMARY KEY, 
                    id INT, 
                    contact_city VARCHAR, 
                    contact_state VARCHAR, 
                    found VARCHAR, 
                    manual VARCHAR, 
                    remove VARCHAR, 
                    still_there VARCHAR
                )
            """,
            "location": """
                CREATE TABLE IF NOT EXISTS location (
                    index INT PRIMARY KEY, 
                    location VARCHAR, 
                    exported FLOAT, 
                    imported FLOAT, 
                    total FLOAT, 
                    inUS BOOL
                )
            """
        }

        for table_name, query in table_queries.items():
            self.cur.execute(query)
            self.conn.commit()
            logging.info(f"Table '{table_name}' created successfully.")
            
    def drop_tables(self):
        """Drop all tables."""
        drop_queries = [
            "DROP TABLE IF EXISTS description",
            "DROP TABLE IF EXISTS travel",
            "DROP TABLE IF EXISTS location"
        ]

        for query in drop_queries:
            self.cur.execute(query)
            self.conn.commit()
            logging.info(f"Dropped table: {query.split()[-1]}")

    def insert_data(self, table_name: str, insert_query: str, data: pd.DataFrame):
        """Insert data into a specific table."""
        for i, row in data.iterrows():
            #print(row)  # Print the row
            self.cur.execute(insert_query, list(row))
        self.conn.commit()
        logging.info(f"Inserted data into {table_name} table.")