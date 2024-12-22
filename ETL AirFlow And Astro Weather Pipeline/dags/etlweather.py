from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from airflow.utils.dates import days_ago
import requests
import json

# latitude and longitude of london
LATITUDE = 51.5074
LONGITUDE = -0.1278
POSTGRES_CONN_ID = "postgres_default"
API_CONN_ID = "open_meteo_api"


## Define the DAG
with DAG(   
    start_date=days_ago(1),
    schedule="@daily",
    catchup=False,
    dag_id="etl_weather_pipeline",
) as dags:


    @task
    def extract_weather_data() -> dict:
        """
        Extracts weather data from the OpenWeather API
        """
        # Create a HttpHook
        http = HttpHook(method="GET", http_conn_id=API_CONN_ID)
        
        # API Endpoint
        ## https://api.open-meteo.com/v1/forecast?latitude=51.5074&longitude=-0.1278&current_weather=true
        
        endpoint=f'/v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}&current_weather=true'

        # Fetch the data
        response = http.run(endpoint)
        
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception("Failed to fetch weather data. message: {response.message}, status_code: {response.status_code}")
    
    @task
    def transform_weather_data(weather_data: dict) -> dict:
        """
        Transforms the weather data
        """
        
        current_weather = weather_data['current_weather']
        transformed_data = {
            'latitude': LATITUDE,
            'longitude': LONGITUDE,
            'temperature': current_weather['temperature'],
            'windspeed': current_weather['windspeed'],
            'winddirection': current_weather['winddirection'],
            'weathercode': current_weather['weathercode']
        }
        return transformed_data
        
    @task
    def load_weather_data(transformed_weather_data: dict):
        """
        Loads the weather data to the database
        """
        # Create a PostgresHook
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        
        # connection
        connection = pg_hook.get_conn()
        
        # cursor
        cursor = connection.cursor()
        
        # connect to the database name etl_weather
        ## cursor.execute("""CREATE DATABASE IF NOT EXISTS etl_weather""")
        ## cursor.execute("""USE etl_weather""")
        
        ## cursor.execute("""
        ##     SELECT 1 FROM pg_database WHERE datname = 'etl_weather';
        ## """)
        ## exists = cursor.fetchone()

        ## if not exists:
        ##    cursor.execute("""CREATE DATABASE etl_weather""")

        
        # Create the table if it does not exist
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS weather (
                id SERIAL PRIMARY KEY,
                latitude FLOAT,
                longitude FLOAT,
                temperature FLOAT,
                windspeed FLOAT,
                winddirection FLOAT,
                weathercode INT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Insert the transformed data
        cursor.execute("""
            INSERT INTO weather (latitude, longitude, temperature, windspeed, winddirection, weathercode)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (transformed_weather_data['latitude'], transformed_weather_data['longitude'], transformed_weather_data['temperature'], transformed_weather_data['windspeed'], transformed_weather_data['winddirection'], transformed_weather_data['weathercode']))
        
        connection.commit()
        cursor.close()
        
    # Define the task dependencies
    weather_data = extract_weather_data()
    transformed_weather_data = transform_weather_data(weather_data)
    load_weather_data(transformed_weather_data)
    
        



