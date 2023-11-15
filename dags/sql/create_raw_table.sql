create schema if not exists raw;

create table raw.nasa_data(
    id varchar(50),
    neo_reference_id varchar(50),
    name varchar(255),
    absolute_magnitude_h NUMERIC(4, 2),
    estimated_diameter_min NUMERIC(20, 10),
    estimated_diameter_max NUMERIC(20, 10),
    metric_date char(10),
    load_dt timestamp default now()
);