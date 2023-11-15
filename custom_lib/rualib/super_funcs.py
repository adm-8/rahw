from contextlib import closing
from random import randint
import json
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.base_hook import BaseHook

from rualib.providers.nasa.hooks.rest_api import NasaRestAPIHook


TEMP_SQL_FILE_PATH_TEMPLATE = "/usr/local/airflow/dags/sql/temp/nasa_data__{ds_nodash}__{custom_run_id}.sql"


def get_nasa_api_key():
    nasa_conn = BaseHook.get_connection("nasa_api")
    return nasa_conn.password


def check_odd_day(**context):
    print("[+] Hello from check_odd_day!")
    ds = context['ds']
    day_num = int(ds[-2:])
    is_even = True if day_num % 2 == 0 else False

    print(f"[*] ds = {ds}; day_num = {day_num}; is_even = {is_even}")

    if is_even:
        # четное число
        return "even_day"
    else:
        # нечетное число
        return "odd_day"


def set_xcom_custom_run_id(**context):
    custom_run_id = randint(1e31 + 1, 1e32)
    context['ti'].xcom_push(key="custom_run_id", value=custom_run_id)


# вообще не прод решение0
def get_filepath(ds, custom_run_id):
    return f"/tmp/nasa_data__{ds}__{custom_run_id}.json"


def get_nasa_data(**context):
    print("[+] Hello from get_nasa_data!")
    print(context)
    ds = context['ds']
    prev_ds = context['prev_ds']
    custom_run_id = context['ti'].xcom_pull(key="custom_run_id", task_ids="set_xcom_custom_run_id")
    NASA_API_KEY = get_nasa_api_key()
    print(f"[+] ds = {ds}; prev_ds = {prev_ds}; NASA_API_KEY = {NASA_API_KEY}; custom_run_id = {custom_run_id}")

    nasa_hook = NasaRestAPIHook()
    response = nasa_hook._get_asteroids_data(start_date=prev_ds, end_date=ds)
    print(f"[+] Response status code: {response.status_code}")

    resp_data = response.text
    print("[+] Response text:")
    print(resp_data)

    file_path = get_filepath(ds, custom_run_id)
    print(f"[+] Writing data to {file_path}")
    with open(file_path, "w") as f:
        f.write(resp_data)


def import_to_stage(**context):
    print("[+] Hello from import_to_stage!")
    print(context)
    ds = context['ds']
    ds_nodash = context['ds_nodash']
    custom_run_id = context['ti'].xcom_pull(key="custom_run_id", task_ids="set_xcom_custom_run_id")

    file_path = get_filepath(ds, custom_run_id)
    print(f"[+] Reading data date from {file_path}")
    with open(file_path, "r") as f:
        data = json.load(f)

    insert_data = []
    stg_table_name = f"stg.nasa_data__{ds_nodash}__{custom_run_id}"

    for date_str in data['near_earth_objects']:
        print(f"[+] Processing data for: {date_str}")
        for row in data['near_earth_objects'][date_str]:
            id = row['id']
            neo_reference_id = row['neo_reference_id']
            name = row['name']
            absolute_magnitude_h = row['absolute_magnitude_h']
            estimated_diameter_min = row['estimated_diameter']['meters']['estimated_diameter_min']
            estimated_diameter_max = row['estimated_diameter']['meters']['estimated_diameter_max']

            formatted_sql = f"""INSERT INTO {stg_table_name} (id, neo_reference_id, name, absolute_magnitude_h, estimated_diameter_min, estimated_diameter_max, metric_date) VALUES ('{id}', '{neo_reference_id}', '{name}', {absolute_magnitude_h}, {estimated_diameter_min}, {estimated_diameter_max}, '{date_str}');"""

            # https://airflow.apache.org/docs/apache-airflow-providers-postgres/stable/_modules/airflow/providers/postgres/hooks/postgres.html#PostgresHook
            # formatted_sql = PostgresHook._generate_insert_sql(
            #     table=stg_table_name,
            #     target_fields=("id", "neo_reference_id", "name", "absolute_magnitude_h", "estimated_diameter_min", "estimated_diameter_max", "metric_date"),
            #     values=(id, neo_reference_id, name, absolute_magnitude_h, estimated_diameter_min, estimated_diameter_max, date_str),
            #     replace=True,
            #     replace_index=["neo_reference_id", "date_str"]
            # )

            insert_data.append(formatted_sql)

    data_sql_string = "; ".join(insert_data)
    print(data_sql_string)

    with closing(PostgresHook("custom_pg").get_conn()) as conn, closing(conn.cursor()) as cur:
        cur.execute(data_sql_string)
        conn.commit()
