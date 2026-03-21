import json
import time
import requests
import pandas as pd

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.http_hook import HttpHook
from airflow.utils.trigger_rule import TriggerRule


# =========================
# CONFIGURATION
# =========================
postgres_conn_id = "postgresql_de"

http_conn = HttpHook.get_connection("http_conn_id")
api_key = http_conn.extra_dejson.get("api_key")
base_url = http_conn.host

nickname = "XXX"
cohort = "XXX"

headers = {
    "X-Nickname": nickname,
    "X-Cohort": cohort,
    "X-Project": "True",
    "X-API-KEY": api_key,
    "Content-Type": "application/x-www-form-urlencoded",
}

default_args = {
    "owner": "denis_ev",
    "email": ["evmenenko.de@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

business_dt = "{{ ds }}"


# =========================
# HELPER FUNCTIONS
# =========================
def log_dq_result(table_name: str, rule_name: str, status: int):
    """
    Writes Data Quality (DQ) check results into dq_checks_results table.

    status:
        0 = success
        1 = failure
    """
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)

    sql = """
        INSERT INTO dq_checks_results (table_name, check_name, check_date, check_result)
        VALUES (%s, %s, current_date, %s)
    """

    hook.run(sql, parameters=(table_name, rule_name, status))


# =========================
# API REQUEST TASKS
# =========================
def create_files_request(ti):
    """
    Sends request to generate a new report.
    Stores task_id in XCom.
    """
    response = requests.post(f"{base_url}/generate_report", headers=headers)
    response.raise_for_status()

    task_id = json.loads(response.content)["task_id"]
    ti.xcom_push(key="task_id", value=task_id)

    print(f"Task ID received: {task_id}")


def get_files(ti):
    """
    Polls API until report is ready and retrieves report_id.
    """
    task_id = ti.xcom_pull(task_ids="create_files_request", key="task_id")

    report_id = None

    for _ in range(20):
        response = requests.get(
            f"{base_url}/get_report?task_id={task_id}",
            headers=headers
        )
        response.raise_for_status()

        payload = json.loads(response.content)
        print(payload)

        if payload["status"] == "SUCCESS":
            report_id = payload["data"]["report_id"]
            break

        time.sleep(10)

    if not report_id:
        raise TimeoutError("Report was not ready in time")

    ti.xcom_push(key="report_id", value=report_id)


# =========================
# DQ RULE 1 - FILE CHECK
# =========================
def dq_rule_1_check_files(date, ti):
    """
    Checks if increment_id exists (i.e., files are available).
    """
    report_id = ti.xcom_pull(task_ids="get_files", key="report_id")

    response = requests.get(
        f"{base_url}/get_increment?report_id={report_id}&date={date}T00:00:00",
        headers=headers
    )
    response.raise_for_status()

    payload = json.loads(response.content)

    increment_id = payload["data"].get("increment_id")

    if not increment_id:
        log_dq_result("all_files", "file_sensor", 1)
        raise ValueError("Increment is empty. Files are not available.")

    ti.xcom_push(key="increment_id", value=increment_id)
    log_dq_result("all_files", "file_sensor", 0)


# =========================
# FILE LOADING
# =========================
def load_csv_from_s3(increment_id: str, filename: str, local_prefix: str):
    """
    Downloads CSV file from S3 and returns it as pandas DataFrame.
    """
    s3_filename = (
        f"https://storage.yandexcloud.net/s3-sprint3/"
        f"cohort_{cohort}/{nickname}/project/{increment_id}/{filename}"
    )

    local_filename = f"{local_prefix}_{filename}"

    response = requests.get(s3_filename)
    response.raise_for_status()

    with open(local_filename, "wb") as f:
        f.write(response.content)

    return pd.read_csv(local_filename)


def load_customer_research(date, ti):
    """
    Loads customer_research data into staging schema.
    """
    increment_id = ti.xcom_pull(task_ids="dq_rule_1_check_files", key="increment_id")

    df = load_csv_from_s3(increment_id, "customer_research_inc.csv", date.replace("-", ""))

    engine = PostgresHook(postgres_conn_id).get_sqlalchemy_engine()
    df.to_sql("customer_research", engine, schema="staging", if_exists="append", index=False)


def load_user_order_log(date, ti):
    """
    Loads user_order_log with basic data cleaning.
    """
    increment_id = ti.xcom_pull(task_ids="dq_rule_1_check_files", key="increment_id")

    df = load_csv_from_s3(increment_id, "user_order_log_inc.csv", date.replace("-", ""))

    if "id" in df.columns:
        df = df.drop("id", axis=1)

    if "uniq_id" in df.columns:
        df = df.drop_duplicates(subset=["uniq_id"])

    if "status" not in df.columns:
        df["status"] = "shipped"

    engine = PostgresHook(postgres_conn_id).get_sqlalchemy_engine()
    df.to_sql("user_order_log", engine, schema="staging", if_exists="append", index=False)


def load_user_activity_log(date, ti):
    """
    Loads user_activity_log with deduplication.
    """
    increment_id = ti.xcom_pull(task_ids="dq_rule_1_check_files", key="increment_id")

    df = load_csv_from_s3(increment_id, "user_activity_log_inc.csv", date.replace("-", ""))

    if "id" in df.columns:
        df = df.drop("id", axis=1)

    if "uniq_id" in df.columns:
        df = df.drop_duplicates(subset=["uniq_id"])

    engine = PostgresHook(postgres_conn_id).get_sqlalchemy_engine()
    df.to_sql("user_activity_log", engine, schema="staging", if_exists="append", index=False)


def load_price_log(date, ti):
    """
    Loads price_log into staging.
    """
    increment_id = ti.xcom_pull(task_ids="dq_rule_1_check_files", key="increment_id")

    df = load_csv_from_s3(increment_id, "price_log_inc.csv", date.replace("-", ""))

    engine = PostgresHook(postgres_conn_id).get_sqlalchemy_engine()
    df.to_sql("price_log", engine, schema="staging", if_exists="append", index=False)


# =========================
# DQ RULES 2-5
# =========================
def dq_rule_2_check_customer_id_order_log():
    """
    Ensures no NULL customer_id in user_order_log.
    """
    hook = PostgresHook(postgres_conn_id)

    result = hook.get_first("""
        SELECT COUNT(*) FROM staging.user_order_log
        WHERE customer_id IS NULL
    """)[0]

    if result > 0:
        log_dq_result("user_order_log", "check_customer_id", 1)
        raise ValueError("NULL customer_id found in user_order_log")

    log_dq_result("user_order_log", "check_customer_id", 0)


def dq_rule_3_check_customer_id_activity_log():
    """
    Ensures no NULL customer_id in user_activity_log.
    """
    hook = PostgresHook(postgres_conn_id)

    result = hook.get_first("""
        SELECT COUNT(*) FROM staging.user_activity_log
        WHERE customer_id IS NULL
    """)[0]

    if result > 0:
        log_dq_result("user_activity_log", "check_customer_id", 1)
        raise ValueError("NULL customer_id found in user_activity_log")

    log_dq_result("user_activity_log", "check_customer_id", 0)


def dq_rule_4_check_test_data_order_log():
    """
    Detects test records in user_order_log.
    """
    hook = PostgresHook(postgres_conn_id)

    result = hook.get_first("""
        SELECT COUNT(*) FROM staging.user_order_log
        WHERE lower(coalesce(status, '')) LIKE '%test%'
    """)[0]

    if result > 0:
        log_dq_result("user_order_log", "check_test_data", 1)
        raise ValueError("Test data found in user_order_log")

    log_dq_result("user_order_log", "check_test_data", 0)


def dq_rule_5_check_test_data_activity_log():
    """
    Detects test records in user_activity_log.
    """
    hook = PostgresHook(postgres_conn_id)

    result = hook.get_first("""
        SELECT COUNT(*) FROM staging.user_activity_log
        WHERE lower(coalesce(action, '')) LIKE '%test%'
    """)[0]

    if result > 0:
        log_dq_result("user_activity_log", "check_test_data", 1)
        raise ValueError("Test data found in user_activity_log")

    log_dq_result("user_activity_log", "check_test_data", 0)


# =========================
# DAG DEFINITION
# =========================
with DAG(
    dag_id="sales_mart_with_dq",
    default_args=default_args,
    description="Sales mart DAG with integrated DQ checks",
    catchup=True,
    start_date=datetime.today() - timedelta(days=7),
    end_date=datetime.today() - timedelta(days=1),
    schedule_interval="@daily",
) as dag:

    start = EmptyOperator(task_id="start")

    create_files_request_task = PythonOperator(
        task_id="create_files_request",
        python_callable=create_files_request,
    )

    get_files_task = PythonOperator(
        task_id="get_files",
        python_callable=get_files,
    )

    dq_rule_1_check_files_task = PythonOperator(
        task_id="dq_rule_1_check_files",
        python_callable=dq_rule_1_check_files,
        op_kwargs={"date": business_dt},
    )

    load_customer_research_task = PythonOperator(
        task_id="load_customer_research",
        python_callable=load_customer_research,
        op_kwargs={"date": business_dt},
    )

    load_user_order_log_task = PythonOperator(
        task_id="load_user_order_log",
        python_callable=load_user_order_log,
        op_kwargs={"date": business_dt},
    )

    load_user_activity_log_task = PythonOperator(
        task_id="load_user_activity_log",
        python_callable=load_user_activity_log,
        op_kwargs={"date": business_dt},
    )

    load_price_log_task = PythonOperator(
        task_id="load_price_log",
        python_callable=load_price_log,
        op_kwargs={"date": business_dt},
    )

    dq_rule_2_check_customer_id_order_log_task = PythonOperator(
        task_id="dq_rule_2_check_customer_id_order_log",
        python_callable=dq_rule_2_check_customer_id_order_log,
    )

    dq_rule_3_check_customer_id_activity_log_task = PythonOperator(
        task_id="dq_rule_3_check_customer_id_activity_log",
        python_callable=dq_rule_3_check_customer_id_activity_log,
    )

    dq_rule_4_check_test_data_order_log_task = PythonOperator(
        task_id="dq_rule_4_check_test_data_order_log",
        python_callable=dq_rule_4_check_test_data_order_log,
    )

    dq_rule_5_check_test_data_activity_log_task = PythonOperator(
        task_id="dq_rule_5_check_test_data_activity_log",
        python_callable=dq_rule_5_check_test_data_activity_log,
    )

    update_dimensions = EmptyOperator(task_id="update_dimensions")
    update_facts = EmptyOperator(task_id="update_facts")
    end = EmptyOperator(task_id="end")

    # =========================
    # DAG FLOW
    # =========================
    (
        start
        >> create_files_request_task
        >> get_files_task
        >> dq_rule_1_check_files_task
    )

    dq_rule_1_check_files_task >> [
        load_customer_research_task,
        load_user_order_log_task,
        load_user_activity_log_task,
        load_price_log_task,
    ]

    load_user_order_log_task >> [
        dq_rule_2_check_customer_id_order_log_task,
        dq_rule_4_check_test_data_order_log_task,
    ]

    load_user_activity_log_task >> [
        dq_rule_3_check_customer_id_activity_log_task,
        dq_rule_5_check_test_data_activity_log_task,
    ]

    [
        load_customer_research_task,
        load_price_log_task,
        dq_rule_2_check_customer_id_order_log_task,
        dq_rule_3_check_customer_id_activity_log_task,
        dq_rule_4_check_test_data_order_log_task,
        dq_rule_5_check_test_data_activity_log_task,
    ] >> update_dimensions

    update_dimensions >> update_facts >> end