create schema if not exists stg;

drop table if exists stg.nasa_data__{{ ds_nodash }}__{{ ti.xcom_pull(key="custom_run_id", task_ids="set_xcom_custom_run_id") }};

create table stg.nasa_data__{{ ds_nodash }}__{{ ti.xcom_pull(key="custom_run_id", task_ids="set_xcom_custom_run_id") }}(
    id varchar(50),
    neo_reference_id varchar(50),
    name varchar(255),
    absolute_magnitude_h NUMERIC(4, 2),
    estimated_diameter_min NUMERIC(20, 10),
    estimated_diameter_max NUMERIC(20, 10),
    metric_date char(10),
    load_dt timestamp default now()
);