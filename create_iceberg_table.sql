-- Iceberg 테이블 생성 쿼리 (Athena에서 실행)
CREATE TABLE bike_db.bike_rental_data (
    rental_id string,
    station_id string,
    station_name string,
    rental_date string,
    return_date string,
    usage_min int,
    distance_meter double,
    birth_year string,
    gender string,
    user_type string,
    district string
)
USING ICEBERG
LOCATION 's3://seoul-bike-iceberg-110230268220-20250731-115044/data/bike_db/bike_rental_data/'
TBLPROPERTIES (
    'table_type'='ICEBERG',
    'format'='parquet',
    'write_compression'='snappy'
);

-- 테이블 확인 쿼리
SHOW TABLES IN bike_db;
DESCRIBE bike_db.bike_rental_data;
SELECT COUNT(*) FROM bike_db.bike_rental_data;
