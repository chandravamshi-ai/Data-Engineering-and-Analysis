import os
import psycopg2
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv(override=True)

# Get PostgreSQL connection details from .env
DB_NAME = os.getenv("POSTGRES_DB")
DB_HOST = os.getenv("POSTGRES_HOST")
DB_PORT = os.getenv("POSTGRES_PORT")
DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")

print("User:", DB_USER)
print("Password:", DB_PASSWORD)

def main():
    try:
        print(DB_HOST,DB_NAME,DB_USER,DB_PASSWORD,DB_PORT)
        # Establish connection
        connection = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        print("Connection successful!")

        # Create a cursor object
        cursor = connection.cursor()

        # Example query
        cursor.execute("SELECT version();")
        version = cursor.fetchone()
        print("PostgreSQL version:", version)

        # Close the cursor and connection
        cursor.close()
        connection.close()

    except Exception as e:
        print("Error while connecting to PostgreSQL:", e)


if __name__ == "__main__":
    main()