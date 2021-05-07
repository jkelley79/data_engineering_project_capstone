import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('config.cfg')

# DROP TABLES

staging_travelers_table_drop = "DROP TABLE IF EXISTS staging_travelers"
staging_airports_table_drop = "DROP TABLE IF EXISTS staging_airports"
staging_cities_table_drop = "DROP TABLE IF EXISTS staging_cities"
staging_temperatures_table_drop = "DROP TABLE IF EXISTS staging_temperatures"

visa_table_drop = "DROP TABLE IF EXISTS visa_codes"
city_table_drop = "DROP TABLE IF EXISTS city"
airports_table_drop = "DROP TABLE IF EXISTS airports"
temperatures_table_drop = "DROP TABLE IF EXISTS temperatures"
statistics_table_drop = "DROP TABLE IF EXISTS statistics"
travelers_table_drop = "DROP TABLE IF EXISTS travelers"

# CREATE TABLES
staging_travelers_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_travelers (
    iata_code VARCHAR,
    age INTEGER,
    visa INTEGER,
    gender VARCHAR,
    year_of_birth INTEGER,
    arrival_year INTEGER,
    arrival_month INTEGER,
    arrival_day INTEGER
    )
""")

staging_airports_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_airports (
    iata_code VARCHAR,
    type VARCHAR,
    name VARCHAR,
    elevation_ft FLOAT,
    city VARCHAR,
    long VARCHAR,
    lat VARCHAR,
    state VARCHAR
    )
""")

staging_cities_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_cities (
    city                       VARCHAR,
    median_age                FLOAT,
    cnt_male                    INTEGER,
    cnt_female                  INTEGER,
    population                  INTEGER,
    cnt_veterans                INTEGER,
    cnt_foreign_born            INTEGER,
    avg_household             FLOAT,
    state                      VARCHAR,
    cnt_white                   INTEGER,
    per_white                 FLOAT,
    cnt_his_latino              INTEGER,
    per_his_latino            FLOAT,
    cnt_asian                   INTEGER,
    per_asian                 FLOAT,
    cnt_amer_ind_ak_native      INTEGER,
    per_amer_ind_ak_native    FLOAT,
    cnt_black                   INTEGER,
    per_black_afr_amer        FLOAT,
    per_male                  FLOAT,
    per_female                FLOAT,
    per_veterans              FLOAT,
    per_foreign_born          FLOAT
    )
""")

staging_temperatures_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_temperatures (
    date                     VARCHAR,
    avg_temp                FLOAT,
    avg_temp_uncertainty    FLOAT,
    city                     VARCHAR,
    lat                      VARCHAR,
    long                     VARCHAR,
    month                     INTEGER,
    year                      INTEGER,
    average_temp_month      FLOAT
    )
""")

# STAGING TABLES

staging_travelers_copy = ("""
copy staging_travelers
from 's3://{}/{}/{}'
iam_role '{}' 
format as csv;
""").format(config['S3']['BUCKET'],config['S3']['FOLDER'],config['OUTPUT']['TRAVELERS'],config['IAM_ROLE']['ARN'])

staging_cities_copy = ("""
copy staging_cities
from 's3://{}/{}/{}'
iam_role '{}' 
format as csv
IGNOREHEADER 1;
""").format(config['S3']['BUCKET'],config['S3']['FOLDER'],config['OUTPUT']['CITIES'],config['IAM_ROLE']['ARN'])

staging_airports_copy = ("""
copy staging_airports
from 's3://{}/{}/{}'
iam_role '{}' 
format as csv
IGNOREHEADER 1;
""").format(config['S3']['BUCKET'],config['S3']['FOLDER'],config['OUTPUT']['AIRPORTS'],config['IAM_ROLE']['ARN'])

staging_temperatures_copy = ("""
copy staging_temperatures
from 's3://{}/{}/{}'
iam_role '{}' 
format as csv
IGNOREHEADER 1;
""").format(config['S3']['BUCKET'],config['S3']['FOLDER'],config['OUTPUT']['TEMPERATURES'],config['IAM_ROLE']['ARN'])


# FINAL TABLES

visa_table_create= ("""
CREATE TABLE IF NOT EXISTS visa_codes (
    v_code INTEGER PRIMARY KEY,
    v_description VARCHAR
    )
""")

visa_table_insert= ("""
INSERT INTO visa_codes (v_code, v_description) 
VALUES (1,'Business'),(2, 'Pleasure'),(3,'Student')
""")

city_table_create= ("""
CREATE TABLE IF NOT EXISTS city (
    c_id BIGINT IDENTITY(1,1),
    c_name VARCHAR,
    c_state_code VARCHAR,
    c_lat VARCHAR,
    c_long VARCHAR
    )
""")

city_table_insert = ("""
INSERT INTO city (c_name, c_state_code) 
SELECT city, state from staging_airports group by city,state
""")

city_table_update = ("""
update city set c_lat = lat, c_long = long
from staging_airports 
where city.c_name = staging_airports.city and city.c_state_code = staging_airports.state
"""
)

airports_table_create=("""
CREATE TABLE IF NOT EXISTS airports (
    a_id BIGINT IDENTITY(1,1),
    a_city_id BIGINT,
    a_iata_code VARCHAR,
    a_type VARCHAR,
    a_name VARCHAR,
    a_elevation_ft FLOAT
    )
""")

airports_table_insert= ("""
INSERT INTO airports (a_city_id, a_iata_code, a_type, a_name, a_elevation_ft) 
SELECT c.c_id, sa.iata_code, sa.type, sa.name, sa.elevation_ft
from staging_airports as sa
join city as c on sa.city = c.c_name and sa.state = c.c_state_code
""")

temperatures_table_create = ("""
CREATE TABLE IF NOT EXISTS temperatures (
    t_city_id   BIGINT,
    t_date      VARCHAR,
    t_month     INTEGER,
    t_year      INTEGER,
    t_avg_temp  FLOAT,
    t_avg_temp_uncertainty FLOAT,
    t_average_temp_month FLOAT
    )
""")

temperatures_table_insert= ("""
INSERT INTO temperatures (t_city_id, t_date, t_month, t_year, t_avg_temp, t_avg_temp_uncertainty, t_average_temp_month) 
SELECT c.c_id, st.date, st.month, st.year, st.avg_temp, st.avg_temp_uncertainty, st.average_temp_month
from staging_temperatures as st
join city as c on st.city = c.c_name
""")

statistics_table_create = ("""
CREATE TABLE IF NOT EXISTS statistics (
    s_city_id   BIGINT,
    s_population                  INTEGER,
    s_median_age                FLOAT,
    s_avg_household             FLOAT,
    s_cnt_male                    INTEGER,
    s_per_male                  FLOAT,
    s_cnt_female                  INTEGER,
    s_per_female                FLOAT,
    s_cnt_veterans                INTEGER,
    s_per_veterans              FLOAT,
    s_cnt_foreign_born            INTEGER,
    s_per_foreign_born          FLOAT,
    s_cnt_white                   INTEGER,
    s_per_white                 FLOAT,
    s_cnt_his_latino              INTEGER,
    s_per_his_latino            FLOAT,
    s_cnt_asian                   INTEGER,
    s_per_asian                 FLOAT,
    s_cnt_amer_ind_ak_native      INTEGER,
    s_per_amer_ind_ak_native    FLOAT,
    s_cnt_black                   INTEGER,
    s_per_black_afr_amer        FLOAT
    )
""")

statistics_table_insert = ("""
INSERT INTO statistics (s_city_id, s_population, s_median_age, s_avg_household, s_cnt_male, s_per_male,
    s_cnt_female, s_per_female, s_cnt_veterans, s_per_veterans, s_cnt_foreign_born, s_per_foreign_born, 
    s_cnt_white, s_per_white, s_cnt_his_latino, s_per_his_latino, s_cnt_asian, s_per_asian, 
    s_cnt_amer_ind_ak_native, s_per_amer_ind_ak_native, s_cnt_black, s_per_black_afr_amer
 ) 
SELECT c.c_id, population, median_age, avg_household, cnt_male, per_male,
    cnt_female, per_female, cnt_veterans, per_veterans, cnt_foreign_born, per_foreign_born, 
    cnt_white, per_white, cnt_his_latino, per_his_latino, cnt_asian, per_asian, 
    cnt_amer_ind_ak_native, per_amer_ind_ak_native, cnt_black, per_black_afr_amer
from staging_cities as sc
join city as c on sc.city = c.c_name and sc.state = c.c_state_code
""")

travelers_table_create = ("""
CREATE TABLE IF NOT EXISTS travelers (
    p_id BIGINT IDENTITY(1,1),
    p_airport_id INTEGER,
    p_age INTEGER,
    p_visa_code INTEGER,
    p_gender VARCHAR,
    p_year_of_birth INTEGER,
    p_arrival_year INTEGER,
    p_arrival_month INTEGER,
    p_arrival_day INTEGER
    )
""")

travelers_table_insert = ("""
INSERT INTO travelers (p_airport_id, p_age, p_visa_code, p_gender, p_year_of_birth, p_arrival_year, p_arrival_month, p_arrival_day)
SELECT a_id, age, visa, gender, year_of_birth, arrival_year, arrival_month, arrival_day
from staging_travelers st
join airports on a_iata_code = st.iata_code
""")

# STAGING VALIDATION QUERIES
staging_airports_validate = "select count(*) from staging_airports"
staging_cities_validate = "select count(*) from staging_cities"
staging_temperatures_validate = "select count(*) from staging_temperatures"
staging_travelers_validate = "select count(*) from staging_travelers"

# VALIDATION QUERIES
airports_validate = "select count(*) from airports"
city_validate = "select count(*) from city"
temperatures_validate = "select count(*) from temperatures"
visa_validate = "select count(*) from visa_codes"
statistics_validate = "select count(*) from statistics"
travelers_validate = "select count(*) from travelers"

# QUERY LISTS

create_table_queries = [staging_travelers_table_create, staging_airports_table_create, staging_cities_table_create,
    staging_temperatures_table_create, visa_table_create, city_table_create, airports_table_create, temperatures_table_create, statistics_table_create, travelers_table_create]
drop_table_queries = [staging_travelers_table_drop, staging_airports_table_drop, staging_cities_table_drop,
    staging_temperatures_table_drop, visa_table_drop, airports_table_drop, city_table_drop, temperatures_table_drop, statistics_table_drop, travelers_table_drop]
copy_table_queries = [staging_travelers_copy, staging_cities_copy, staging_airports_copy, staging_temperatures_copy]
insert_table_queries = [visa_table_insert, city_table_insert, city_table_update, airports_table_insert, temperatures_table_insert, statistics_table_insert, travelers_table_insert]
validation_queries  = [visa_validate, city_validate, airports_validate, temperatures_validate, statistics_validate, travelers_validate]
staging_validation_queries  = [staging_airports_validate, staging_cities_validate, staging_temperatures_validate, staging_travelers_validate]