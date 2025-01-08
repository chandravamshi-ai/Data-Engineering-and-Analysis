from typing import Tuple
import psycopg2
import logging
 
class DatabaseHandler:
    def __init__(self, db_config: dict):
        self.db_config = db_config

    def create_database(self) -> Tuple[psycopg2.extensions.cursor, psycopg2.extensions.connection]:
        """Create and connect to the dog_adoption database."""
        try:
            # Connect to default 'postgres' database
            conn = psycopg2.connect(**self.db_config)
            conn.set_session(autocommit=True)
            cur = conn.cursor()

            # Create the new database
            cur.execute("DROP DATABASE IF EXISTS dog_adoption")
            cur.execute("CREATE DATABASE dog_adoption")

            # Close connection to the default database
            conn.close()

            # Connect to the new database
            self.db_config["dbname"] = "dog_adoption"
            conn = psycopg2.connect(**self.db_config)
            cur = conn.cursor()
            conn.set_session(autocommit=True)

            logging.info("Database dog_adoption created successfully.")
            return cur, conn

        except Exception as e:
            logging.error(f"Error creating database: {e}")
            raise

    def execute_query(self, query: str):
        """Execute a single SQL query."""
        with psycopg2.connect(**self.db_config) as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                conn.commit()
                logging.info(f"Executed query: {query}")

    def close_connection(self, conn):
        """Close the database connection."""
        conn.close()
        logging.info("Database connection closed.")