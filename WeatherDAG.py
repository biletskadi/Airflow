from airflow import DAG
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.models import Variable
import json

default_args = {
    'owner': 'Diana',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'my_weather_dag',
    default_args=default_args,
    description='DAG for fetching and processing weather data',
    schedule_interval=timedelta(days=1),
)

b_create = SqliteOperator(
    task_id="create_table_sqlite",
    sqlite_conn_id="airflow_conn",
    sql="""
     CREATE TABLE IF NOT EXISTS measures
     (
     timestamp TIMESTAMP,
     city VARCHAR(255),
     temperature FLOAT,
     humidity FLOAT,
     cloudiness FLOAT,
     wind_speed FLOAT
     );""",
    dag=dag,
)

cities = ["Lviv", "Kyiv", "Kharkiv", "Odesa", "Zhmerynka"]

for city in cities: 
    check_api_param = HttpSensor(
        task_id=f"check_api_coord_{city}",
        http_conn_id="weather_conn",
        endpoint="data/2.5/weather",
        request_params={"appid": Variable.get("WEATHER_API_KEY"), "q": city},
        mode="poke",
        dag=dag,
    )

    extract_data_param = SimpleHttpOperator(
        task_id=f"extract_data_coord_{city}",
        http_conn_id="weather_conn",
        endpoint="data/2.5/weather",
        data={"appid": Variable.get("WEATHER_API_KEY"), "q": city},
        method="GET",
        response_filter=lambda x: json.loads(x.text),
        log_response=True,
        dag=dag,
    )

    def _process_param(ti, **kwargs):
        timestamp = kwargs['ts']
        info = ti.xcom_pull(task_ids=f'extract_data_coord_{city}', key='return_value')
        lon = info["coord"]["lon"]
        lat = info["coord"]["lat"]
        return timestamp, lon, lat
    
    process_param = PythonOperator(
        task_id=f"process_data_coord_{city}",
        python_callable=_process_param,
        provide_context=True,
        dag=dag,
    )
    
    check_api = HttpSensor(
        task_id=f"check_api_{city}",
        http_conn_id="weather_conn",
        endpoint="data/3.0/onecall",
        request_params={"lat": f"{{{{ti.xcom_pull(task_ids=f'process_data_coord_{city}')[1]}}}}", "lon": f"{{{{ti.xcom_pull(task_ids='process_data_coord_{city}')[0]}}}}", 'dt': f"{{{{ti.xcom_pull(task_ids='process_data_coord_{city}')[0]}}}}", "appid": Variable.get("WEATHER_API_KEY")},
        mode="poke",
        dag=dag,
    )

    extract_data = SimpleHttpOperator(
        task_id=f"extract_data_{city}",
        http_conn_id="weather_conn",
        endpoint="data/3.0/onecall",
        data={"lat": f"{{{{ti.xcom_pull(task_ids=f'process_data_coord_{city}')[1]}}}}", "lon": f"{{{{ti.xcom_pull(task_ids='process_data_coord_{city}')[0]}}}}", 'dt': f"{{{{ti.xcom_pull(task_ids='process_data_coord_{city}')[0]}}}}", "appid": Variable.get("WEATHER_API_KEY")},
        method="GET",
        response_filter=lambda x: json.loads(x.text),
        log_response=True,
        dag=dag,
    )

    def _process_weather(ti, **kwargs):
        info = ti.xcom_pull(task_ids=f'extract_data_{city}', key='return_value')
        timestamp = info["current"]["dt"]
        temp = info["current"]["temp"]
        humidity = info["current"]["humidity"]
        cloudiness = info["current"]["clouds"]
        wind_speed = info["current"]["wind_speed"]
        return timestamp, temp, humidity, cloudiness, wind_speed

    process_data = PythonOperator(
        task_id=f"process_data_{city}",
        python_callable=_process_weather,
        provide_context=True,
        dag=dag,
    )

    inject_data = SqliteOperator(
        task_id=f"inject_data_{city}",
        sqlite_conn_id="airflow_conn",
        sql=f"""
        INSERT INTO measures (timestamp, city, temperature, humidity, cloudiness, wind_speed) VALUES
        ('{{{{ti.xcom_pull(task_ids="process_data_{city}")[0]}}}}', '{city}',
        '{{{{ti.xcom_pull(task_ids="process_data_{city}")[1]}}}}',
        '{{{{ti.xcom_pull(task_ids="process_data_{city}")[2]}}}}',
        '{{{{ti.xcom_pull(task_ids="process_data_{city}")[3]}}}}',
        '{{{{ti.xcom_pull(task_ids="process_data_{city}")[4]}}}}');
        """,
        dag=dag,
    )

    b_create >> check_api_param >> extract_data_param >> process_param >> check_api >> extract_data >> process_data >> inject_data