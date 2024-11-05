from airflow import DAG
from airflow.providers.https.hooks.http import HttpHook 
from airflow.providers.postgres.hooks.postgres import PostgresHook 
from airflow.decorators import task
from airflow.utils.dates import days_ago

# Long and lati of ASU
LONGITUDE = '-111.928001'
LATITUDE = '33.424564'
POSTGRES_CONN_ID = 'postgres_default'
API_CONN_ID = 'open_metei_api'

default_args={
    'owner':'airflow',
    'start_date': days_ago(1)
}

#DAG
with DAG(dag_id = 'weather_etl_pipeline',
         schedule_interval="@daily",
         catchup=False,
         default_args=default_args) as dags:
    
    @task # the extraction. 
    def extraction():
        '''using the a weather API for this demonstration.'''

        http_hook = HttpHook(http_conn_id = API_CONN_ID, method = 'GET')

        ## building the api endpoint
        endpoint = f'/v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}&current_weather=true'

        # we can now make a request
        response = http_hook.run(endpoint)

        if response.status_code == 200:
            return response.json()
        else: 
            raise Exception(f'Failed to fetch weather data; {response.status_code}')
        
    @task() # the transformation
    def transformation(weather_data):
        """Transform the extracted weather data."""
        current_weather = weather_data['current_weather']
        # collecting the data from the api. 
        transformed_data = {
            'latitude': LATITUDE,
            'longitude': LONGITUDE,
            'temperature': current_weather['temperature'],
            'windspeed': current_weather['windspeed'],
            'winddirection': current_weather['winddirection'],
            'weathercode': current_weather['weathercode']
        }
        return transformed_data
    
    @task() # loading to the DB 
    def load(transformed_data):
        """Load transformed data into PostgreSQL."""
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        # Creating a table 
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS weather_data (
            latitude FLOAT,
            longitude FLOAT,
            temperature FLOAT,
            windspeed FLOAT,
            winddirection FLOAT,
            weathercode INT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)

        # Insert transformed data into the table
        cursor.execute("""
        INSERT INTO weather_data (latitude, longitude, temperature, windspeed, winddirection, weathercode)
        VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            transformed_data['latitude'],
            transformed_data['longitude'],
            transformed_data['temperature'],
            transformed_data['windspeed'],
            transformed_data['winddirection'],
            transformed_data['weathercode']
        ))

        conn.commit()
        cursor.close()

    ## DAG Worflow- ETL Pipeline
    weather_data= extraction()
    transformed_data=transformation(weather_data)
    load(transformed_data)