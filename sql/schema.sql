-- Star schema for the NYC Taxi warehouse (PostgreSQL)

CREATE TABLE IF NOT EXISTS dim_date (
    date_key    INT PRIMARY KEY,
    full_date   DATE NOT NULL,
    year        INT,
    quarter     INT,
    month       INT,
    month_name  VARCHAR(20),
    week        INT,
    day         INT,
    day_name    VARCHAR(20),
    is_weekend  BOOLEAN
);

CREATE TABLE IF NOT EXISTS dim_location (
    location_key   INT PRIMARY KEY,
    location_id    INT,
    borough        VARCHAR(50),
    zone           VARCHAR(100),
    service_zone   VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS dim_payment (
    payment_key         INT PRIMARY KEY,
    payment_type         INT,
    payment_description  VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS dim_vendor (
    vendor_key   INT PRIMARY KEY,
    vendor_id    INT,
    vendor_name  VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS fact_trips (
    trip_id               BIGINT PRIMARY KEY,
    date_key              INT REFERENCES dim_date(date_key),
    pickup_location_key   INT REFERENCES dim_location(location_key),
    dropoff_location_key  INT REFERENCES dim_location(location_key),
    vendor_key            INT REFERENCES dim_vendor(vendor_key),
    payment_key           INT REFERENCES dim_payment(payment_key),
    rate_code_key         INT,
    passenger_count       INT,
    trip_distance         NUMERIC(8,2),
    trip_duration         NUMERIC(10,2),
    fare_amount           NUMERIC(10,2),
    tip_amount            NUMERIC(10,2),
    tolls_amount          NUMERIC(10,2),
    total_amount          NUMERIC(10,2),
    pickup_borough        VARCHAR(50),
    pickup_zone           VARCHAR(100),
    dropoff_borough       VARCHAR(50),
    dropoff_zone          VARCHAR(100)
);

CREATE INDEX IF NOT EXISTS idx_fact_trips_date ON fact_trips(date_key);
CREATE INDEX IF NOT EXISTS idx_fact_trips_pickup_loc ON fact_trips(pickup_location_key);

CREATE TABLE IF NOT EXISTS etl_audit (
    batch_id         VARCHAR(20),
    file_name        VARCHAR(255),
    processing_date  TIMESTAMP,
    record_count     BIGINT,
    status           VARCHAR(20),
    start_time       TIMESTAMP,
    end_time         TIMESTAMP,
    error_message    TEXT
);
