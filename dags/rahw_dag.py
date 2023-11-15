import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator

from airflow.operators.postgres_operator import PostgresOperator

from rualib.super_funcs import check_odd_day, get_nasa_data, set_xcom_custom_run_id, import_to_stage
from rualib.providers.nasa.sensors.rest_api import NasaRestAPISensor


with DAG(
        dag_id='raif_airflow_home_work',
        schedule_interval='0 3 * * *',
        start_date=datetime.datetime(2023, 11, 1),
        catchup=True,
        dagrun_timeout=datetime.timedelta(minutes=60),
        tags=['example', 'example2'],
        params={"example_key": "example_value"},
) as dag:
    _start = EmptyOperator(
        task_id='start',
    )

    _end = EmptyOperator(
        task_id='end',
        trigger_rule="all_done",
    )

    _even_day = EmptyOperator(
        task_id='even_day',
    )

    _odd_day = EmptyOperator(
        task_id='odd_day',
    )

    _check_odd_day = BranchPythonOperator(
        task_id='check_odd_day',
        provide_context=True,
        python_callable=check_odd_day
    )

    _set_xcom_custom_run_id = PythonOperator(
        task_id='set_xcom_custom_run_id',
        provide_context=True,
        python_callable=set_xcom_custom_run_id
    )

    _check_nasa_data_prev = NasaRestAPISensor(
        task_id='check_nasa_data_prev',
        s_date='{{ prev_ds }}',
        e_date='{{ prev_ds }}',
    )

    _all_success = EmptyOperator(
        task_id='all_success',
        trigger_rule="all_success",
    )

    _check_nasa_data_cur = NasaRestAPISensor(
        task_id='check_nasa_data_cur',
        s_date='{{ ds }}',
        e_date='{{ ds }}',
    )

    _all_done = EmptyOperator(
        task_id='all_done',
        trigger_rule="all_done",
    )

    _get_nasa_data = PythonOperator(
        task_id='get_nasa_data',
        provide_context=True,
        python_callable=get_nasa_data
    )

    _create_stg_table = PostgresOperator(
        task_id='create_stg_table',
        postgres_conn_id='custom_pg',
        sql="sql/create_stg_table.sql"
    )

    _import_to_stage = PythonOperator(
        task_id='import_to_stage',
        provide_context=True,
        python_callable=import_to_stage
    )

    _stage_to_raw = PostgresOperator(
        task_id='stage_to_raw',
        postgres_conn_id='custom_pg',
        sql="""
        delete from raw.nasa_data where metric_date in ('{{ ds }}', '{{ prev_ds }}');
        
        insert into raw.nasa_data
        select * from stg.nasa_data__{{ ds_nodash }}__{{ ti.xcom_pull(key="custom_run_id", task_ids="set_xcom_custom_run_id") }};
        """
    )

    _drop_stg_table = PostgresOperator(
        task_id='drop_stg_table',
        postgres_conn_id='custom_pg',
        sql="""drop table if exists stg.nasa_data__{{ ds_nodash }}__{{ ti.xcom_pull(key="custom_run_id", task_ids="set_xcom_custom_run_id") }}"""
    )

    # Запускаем процесс, проверяем четный лм день
    _start >> _check_odd_day >> [_even_day, _odd_day]

    # если день нечетный, то погнали по процессу
    _odd_day >> _set_xcom_custom_run_id >> [_check_nasa_data_cur, _check_nasa_data_prev]

    _check_nasa_data_cur >> _all_done
    _check_nasa_data_prev >> _all_success

    year = '3333'
    for dt in [
        f"{year}-01-01",
        f"{year}-01-02",
        f"{year}-01-03",
    ]:
        _loop_sensor = NasaRestAPISensor(
            task_id=f"loop_sensor_{dt.replace('-', '_')}",
            poke_interval=5,
            mode="reschedule",
            retries=5,
            s_date=dt,
            e_date=dt,
        )

        _set_xcom_custom_run_id >> _loop_sensor >> _all_success

    [_all_done, _all_success] >> _get_nasa_data >> _create_stg_table >> _import_to_stage >> _stage_to_raw >> _drop_stg_table >> _end

    # если день четный, то уходим в конец
    _even_day >> _end
