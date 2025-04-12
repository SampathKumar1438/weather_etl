from datetime import datetime, timedelta
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from pathlib import Path

CITY_NAME = 'Coimbatore'
LOCAL_OUTPUT_PATH = str(Path.home() / 'airflow' / 'output')

def extract(**kwargs):

    extracted = {
        'city': CITY_NAME,
        'temperature_c': 28.5,  # Celsius
        'weather': 'clear sky',
        'timestamp': datetime.utcnow().isoformat()  # current timestamp
    }
    print("Extracted data:", extracted)
    return extracted

# Transform function: Celsius to Fahrenheit conversion
def transform(**kwargs):
    data = kwargs['ti'].xcom_pull(task_ids='extract_task')
    print("Data before transformation:", data)
    data['temperature_f'] = round(data['temperature_c'] * 9 / 5 + 32, 2)  # Celsius to Fahrenheit
    print("Transformed data:", data)
    return data

# Load function: Save the data to a local CSV
def load(**kwargs):
    data = kwargs['ti'].xcom_pull(task_ids='transform_task')
    print("Data to be saved:", data)
    df = pd.DataFrame([data])

    # Ensure the output directory exists
    Path(LOCAL_OUTPUT_PATH).mkdir(parents=True, exist_ok=True)

    # Save data as CSV
    timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
    filename = f"{LOCAL_OUTPUT_PATH}/weather_{timestamp}.csv"
    df.to_csv(filename, index=False)
    print(f"Saved data to {filename}")

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# Define the DAG and tasks
with DAG(
    dag_id='weather_etl_dag',
    default_args=default_args,
    schedule_interval='@hourly',  # Run the DAG every hour
    catchup=False,
    description='Extract mock weather data and save to local CSV',
) as dag:

    # Extract task: Get the mock weather data
    extract_task = PythonOperator(
        task_id='extract_task',
        python_callable=extract,
        provide_context=True
    )

    # Transform task: Convert Celsius to Fahrenheit
    transform_task = PythonOperator(
        task_id='transform_task',
        python_callable=transform,
        provide_context=True
    )

    # Load task: Save the data to CSV
    load_task = PythonOperator(
        task_id='load_task',
        python_callable=load,
        provide_context=True
    )

    # Define task dependencies: extract -> transform -> load
    extract_task >> transform_task >> load_task
