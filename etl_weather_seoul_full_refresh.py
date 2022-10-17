from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook

from datetime import datetime
from datetime import timedelta

import requests
import logging


def get_Redshift_connection(autocommit=False):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit

    return conn.cursor()


def extract(**context):
    api_key = context["params"]["api_key"]
    
    # 아래 api로 알아낸 위도, 경도
    # https://openweathermap.org/api/geocoding-api
    lat = 37.5666791
    lon = 126.9782914
    response = requests.get(f"https://api.openweathermap.org/data/2.5/onecall?lat={lat}&lon={lon}&exclude=current,minutely,hourly&appid={api_key}&units=metric&lang=kr")
    
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(response.json())


def transform(**context):
    data = context["task_instance"].xcom_pull(key="return_value", task_ids="extract")
    lines = []
        
    # 오늘 날씨 제외하고 7일간의 날씨 예보 가져오기
    for d in data["daily"][1:]:
        date = datetime.fromtimestamp(d["dt"]).strftime("%Y-%m-%d %H:%M")
        temp = float(d["temp"]["day"])
        min_temp = float(d["temp"]["min"])
        max_temp = float(d["temp"]["max"])
        lines.append((date, temp, min_temp, max_temp))
        logging.info(f"date: {date}, temp:{temp}, min_temp: {min_temp}, max_temp: {max_temp}")

    return lines


def load(**context):
    schema = context["params"]["schema"]
    table = context["params"]["table"]

    cur = get_Redshift_connection()
    lines = context["task_instance"].xcom_pull(key="return_value", task_ids="transform")

    # Full Refresh
    # 매번 향후 일주일의 데이터만 테이블에 유지
    sql = f"BEGIN; DELETE FROM {schema}.{table};"
    for date, temp, min_temp, max_temp in lines:
        sql += f"INSERT INTO {schema}.{table} VALUES ('{date}',{temp},{min_temp},{max_temp});"
    sql += "END;"
    logging.info(sql)
    cur.execute(sql)


dag_weather = DAG(
    dag_id = "weather_full_refresh",
    start_date = datetime(2022, 10, 12),
    schedule_interval = "0 0 * * *",
    max_active_runs = 1,
    catchup = False,
    default_args = {
        "retries": 1,
        "retry_delay": timedelta(minutes=3)
    }
)


extract = PythonOperator(
    task_id = "extract",
    python_callable = extract,
    params = {
        "api_key": Variable.get("open_weather_api_key")
    },
    dag = dag_weather
)

transform = PythonOperator(
    task_id = "transform",
    python_callable = transform,
    dag = dag_weather
)

load = PythonOperator(
    task_id = "load",
    python_callable = load,
    params = {
        "schema": "gldkzm",
        "table": "weather_forecast"
    },
    dag = dag_weather
)

extract >> transform >> load