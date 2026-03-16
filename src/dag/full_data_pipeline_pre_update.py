import datetime
import time
import psycopg2
import requests
import json
import pandas as pd
import numpy as np

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.models.xcom import XCom
from psycopg2.extras import execute_values

# --- API Settings ---
api_conn = BaseHook.get_connection('create_files_api')
api_endpoint = api_conn.host
api_token = api_conn.password

nickname = 'XXX'  
cohort = 'XXX' 
api_token = 'XXX'

headers = {
    "X-API-KEY": api_token,
    "X-Nickname": nickname,
    "X-Cohort": cohort
}

# --- PostgreSQL connection ---
psql_conn = BaseHook.get_connection('pg_connection')


# --- 1. Generate report ---
def create_files_request(ti, headers):
    api_conn = BaseHook.get_connection('create_files_api')
    api_endpoint = api_conn.host

    method_url = '/generate_report'
    r = requests.post(f'https://{api_endpoint}{method_url}', headers=headers)

    if r.status_code != 200:
        raise Exception(f"API request failed with status {r.status_code}: {r.text}")

    response_dict = r.json()
    task_id = response_dict['task_id']
    ti.xcom_push(key='task_id', value=task_id)
    print(f"task_id is {task_id}")
    return task_id


# --- 2. Check report readiness ---
def check_report(ti):
    task_id = ti.xcom_pull(key='task_id', task_ids='create_files_request')
    report_id = None
    for i in range(4):
        time.sleep(20)
        r = requests.get(f'https://{api_endpoint}/get_report', params={'task_id': task_id}, headers=headers)
        response_dict = r.json()
        print(i, response_dict.get('status'))
        if response_dict.get('status') == 'SUCCESS':
            report_id = response_dict['data']['report_id']
            break
    if not report_id:
        raise Exception("Report not ready")
    ti.xcom_push(key='report_id', value=report_id)
    print(f"report_id: {report_id}")
    return report_id


# --- 3. Upload data from S3 to PostgreSQL ---
def upload_from_s3_to_pg(ti, nickname, cohort):
    report_ids = ti.xcom_pull(key='report_id', task_ids=['check_report'])
    report_id = report_ids[0]

    storage_url = "https://storage.yandexcloud.net/s3-sprint3/cohort_{COHORT_NUMBER}/{NICKNAME}/{REPORT_ID}/{FILE_NAME}"
    personal_storage_url = storage_url.replace("{COHORT_NUMBER}", cohort).replace("{NICKNAME}", nickname).replace("{REPORT_ID}", report_id)

    # --- Connect to PostgreSQL ---
    psql_conn = BaseHook.get_connection('pg_connection')
    conn = psycopg2.connect(
        dbname='de',
        user=psql_conn.login,
        password=psql_conn.password,
        host=psql_conn.host,
        port=psql_conn.port
    )
    cur = conn.cursor()

    # --- Truncate staging tables ---
    cur.execute("""
        TRUNCATE stage.customer_research;
        TRUNCATE stage.user_order_log;
        TRUNCATE stage.user_activity_log;
    """)
    conn.commit()

    # ---------------------------------------------------
    # CUSTOMER_RESEARCH
    # ---------------------------------------------------
    df_customer_research = pd.read_csv(personal_storage_url.replace("{FILE_NAME}", "customer_research.csv"))
    df_customer_research = df_customer_research[["date_id","category_id","geo_id","sales_qty","sales_amt"]]
    df_customer_research = df_customer_research.replace({np.nan: None})
    records = list(df_customer_research.itertuples(index=False, name=None))

    insert_query = """
        INSERT INTO stage.customer_research
        (date_id, category_id, geo_id, sales_qty, sales_amt)
        VALUES %s
    """
    execute_values(cur, insert_query, records)
    conn.commit()
    print("customer_research loaded")

    # ---------------------------------------------------
    # USER_ORDER_LOG
    # ---------------------------------------------------
    df_order_log = pd.read_csv(personal_storage_url.replace("{FILE_NAME}", "user_order_log.csv"))
    df_order_log = df_order_log[["date_time", "city_id", "city_name", "customer_id",
                                 "first_name", "last_name", "item_id", "item_name",
                                 "quantity", "payment_amount"]]
    df_order_log = df_order_log.replace({np.nan: None})
    records = list(df_order_log.itertuples(index=False, name=None))

    insert_query = """
        INSERT INTO stage.user_order_log
        (date_time, city_id, city_name, customer_id, first_name, last_name, item_id, item_name, quantity, payment_amount)
        VALUES %s
    """
    execute_values(cur, insert_query, records)
    conn.commit()
    print("user_order_log loaded")

    # ---------------------------------------------------
    # USER_ACTIVITY_LOG
    # ---------------------------------------------------
    df_activity_log = pd.read_csv(personal_storage_url.replace("{FILE_NAME}", "user_activity_log.csv"))
    df_activity_log = df_activity_log[["date_time","action_id","customer_id","quantity"]]
    df_activity_log = df_activity_log.replace({np.nan: None})
    records = list(df_activity_log.itertuples(index=False, name=None))

    insert_query = """
        INSERT INTO stage.user_activity_log
        (date_time, action_id, customer_id, quantity)
        VALUES %s
    """
    execute_values(cur, insert_query, records)
    conn.commit()
    print("user_activity_log loaded")

    cur.close()
    conn.close()
    return 200


# --- 4. Update d_* tables ---
def update_mart_d_tables(ti):
    conn = psycopg2.connect(
        dbname='de',
        user=psql_conn.login,
        password=psql_conn.password,
        host=psql_conn.host,
        port=psql_conn.port
    )
    cur = conn.cursor()

    # Truncate d_* tables
    cur.execute("""
        DELETE FROM mart.f_activity;
        DELETE FROM mart.f_daily_sales;
        DELETE FROM mart.f_research;
        DELETE FROM mart.d_calendar;
        DELETE FROM mart.d_customer;
        DELETE FROM mart.d_item;
        DELETE FROM mart.d_city;
        DELETE FROM mart.d_category;
    """)
    conn.commit()

    # Insert d_calendar
    cur.execute("""
        WITH all_dates AS (
            SELECT date_time::date AS fact_date FROM stage.user_order_log
            UNION
            SELECT date_time::date FROM stage.user_activity_log
            UNION
            SELECT date_id::date FROM stage.customer_research
        )
        INSERT INTO mart.d_calendar (date_id, fact_date, day_num, month_num, month_name, year_num)
        SELECT DISTINCT TO_CHAR(fact_date, 'YYYYMMDD')::INT, fact_date, EXTRACT(DAY FROM fact_date),
                        EXTRACT(MONTH FROM fact_date), TO_CHAR(fact_date, 'Mon'), EXTRACT(YEAR FROM fact_date)
        FROM all_dates;
    """)
    conn.commit()

    # Insert d_customer
    cur.execute("""
        INSERT INTO mart.d_customer (customer_id, first_name, last_name, city_id)
        SELECT customer_id, MAX(first_name), MAX(last_name), MAX(city_id)
        FROM stage.user_order_log
        GROUP BY customer_id;
    """)
    conn.commit()

    # Insert d_item
    cur.execute("""
        INSERT INTO mart.d_item (item_id, item_name, category_id)
        SELECT DISTINCT item_id, item_name, 0
        FROM stage.user_order_log;
    """)
    conn.commit()

    # Insert d_city
    cur.execute("""
        INSERT INTO mart.d_city (city_id, city_name)
        SELECT DISTINCT city_id, city_name
        FROM stage.user_order_log;
    """)
    conn.commit()

    # Insert d_category
    cur.execute("""
        INSERT INTO mart.d_category (category_id, category_name)
        SELECT DISTINCT category_id, 'category_' || category_id
        FROM stage.customer_research
        WHERE category_id IS NOT NULL;
    """)
    conn.commit()

    cur.close()
    conn.close()
    return 200


# --- 5. Update f_* tables ---
def update_mart_f_tables(ti):
    conn = psycopg2.connect(
        dbname='de',
        user=psql_conn.login,
        password=psql_conn.password,
        host=psql_conn.host,
        port=psql_conn.port
    )
    cur = conn.cursor()


    conn.commit()

    # Insert f_activity
    cur.execute("""
        INSERT INTO mart.f_activity (activity_id, date_id, click_number)
        SELECT action_id, TO_CHAR(date_time::date, 'YYYYMMDD')::INT, SUM(quantity)
        FROM stage.user_activity_log
        GROUP BY action_id, TO_CHAR(date_time::date, 'YYYYMMDD')::INT;
    """)
    conn.commit()

    # Insert f_daily_sales
    cur.execute("""
        INSERT INTO mart.f_daily_sales (date_id, item_id, customer_id, price, quantity, payment_amount)
        SELECT TO_CHAR(date_time::date, 'YYYYMMDD')::INT, item_id, customer_id,
               SUM(payment_amount)/NULLIF(SUM(quantity),0), SUM(quantity), SUM(payment_amount)
        FROM stage.user_order_log
        GROUP BY TO_CHAR(date_time::date, 'YYYYMMDD')::INT, item_id, customer_id;
    """)
    conn.commit()

    # Insert f_research
    cur.execute("""
        INSERT INTO mart.f_research (date_id, category_id, geo_id, customer_id, item_id, quantity, amount)
        SELECT TO_CHAR(date_id::date, 'YYYYMMDD')::INT, category_id, geo_id, 0, 0, SUM(sales_qty), SUM(sales_amt)
        FROM stage.customer_research
        GROUP BY TO_CHAR(date_id::date, 'YYYYMMDD')::INT, category_id, geo_id;
    """)
    conn.commit()

    cur.close()
    conn.close()
    return 200


# --- DAG ---
dag = DAG(
    dag_id='591_full_dag',
    schedule_interval='0 0 * * *',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60)
)

# --- Operators ---
t_file_request = PythonOperator(
    task_id='create_files_request',
    python_callable=create_files_request,
    op_kwargs={"headers": headers},
    dag=dag
)

t_check_report = PythonOperator(
    task_id='check_report',
    python_callable=check_report,
    dag=dag
)

t_upload_from_s3_to_pg = PythonOperator(
    task_id='upload_from_s3_to_pg',
    python_callable=upload_from_s3_to_pg,
    op_kwargs={"nickname": nickname, "cohort": cohort},
    dag=dag
)

t_update_mart_d_tables = PythonOperator(
    task_id='update_mart_d_tables',
    python_callable=update_mart_d_tables,
    dag=dag
)

t_update_mart_f_tables = PythonOperator(
    task_id='update_mart_f_tables',
    python_callable=update_mart_f_tables,
    dag=dag
)

# --- Task sequence ---
t_file_request >> t_check_report >> t_upload_from_s3_to_pg >> t_update_mart_d_tables >> t_update_mart_f_tables