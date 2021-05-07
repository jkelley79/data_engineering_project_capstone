# Introduction

The purpose of this project is to analyze international traveler information to the United States in part with city, temperature, and airport data. This analysis can be used to determine any patterns between traveler age, sex, visa type and travel dates with data about the destinations within the US.

# Purpose

The scripts in this project are intended to process input data from S3 or elsewhere, transform that data using Pandas and Spark, to then be written to S3 and loaded into Redshift for analysis. 

input_data folder - Contains sample files containing data used to develop this ETL pipeline
output_data folder - Results from last run of the dataprep script
create_tables.py - Script to create both staging tables and fact/dimension tables schema
config.cfg - Configuration file with all information used to determine the location of input/output buckets, Redshift connection and AWS credentials.
dataprep.py - Script to load data from files into Pandas and Spark dataframes clean it, and transform it into expected staging data model
etl.py - ETL script which loads the data from locations specified in dwh.cfg into Redshift in staging tables and then loads data to a new set of fact/dimension tables 
README.md - This file containing information about this project
sql_queries.py - Script containing SQL queries
immigration_data_sample.csv - This file was part of the workspace but is not used.

# Scope of this project

The scope of this project is to take the data around international travel to the US, combine it with statistics about US cities based on demographic information as well as weather temperatures and determine the relationships between the data sources. Examples include figuring out where certain age groups traveled to the most and was it possibly due to temperatures. Also did demographic information dictate where certain travelers might head based on time of the travel. 

Next steps would be to bring up an Airflow instance to convert this ETL pipeline into a DAG that could be run on the periodic basis as mentioned in the additional considerations section. This would have allowed the processing of the files especially the travelers files by month rather than all at once.

# Explore and Assess the Data

- Assessment of cities data
1. Race counts are stored in multiple rows rather than a single row and multiple columns. 
2. Information about veterans, foreign born, gender and race were given in total counts where percentages might be more interesting.
3. Additional statistics of median age and household size might also be useful if other items are in percentages.

- Assessment of airport data
1. IATA code which will be important for determining where people are traveling will need to be filtered out to valid values.
2. Coordinates information is stored together and in negative/positive format rather than N/S E/W

- Assessment of the temperature data
1. Temperature data goes back much further than necessary but possibly could be used to calculate a better monthly average across all years
2. Temperature data is stored for all cities when for us really only US data is useful since we don't know exactly where people are coming from so we need to filter tha out.

- Assessment of traveler data
1. Data is provided in SAS file format and large files for each month so initially we may only want to use one month before importing all the data.
2. Filter out where IATA codes are not valid such as 'XXX'
3. SAS dates need to be formatted by adding the number of days to 1/1/1960
4. Formatting of the files needs to be changed to break out travelers by year, month, and day.
5. All schema files are not exactly the same as some have more columns than others which makes reading them into the same dataframe difficult so they would need to be processed separately. 

# Data Model
The data model is broken up into four staging tables and then for the analytic data it is broken up into five dimension tables and one fact table.

For the staging tables each database table is a carbon copy of the data within the exported CSV files. 

For the star schema we have the following tables and data attributes:

Visa codes - Dimension table - Directly inserted from the immigration data explanation around visa codes to descriptions
- Columns
    - v_code (integer) code
    - v_description (varchar) description of the code

City - Dimension table - Inserted by selecting city and state from airports and then updating with lat/long since airports are the main reference for our traveler data
- Columns
    - c_id (bigint) index column
    - c_name (varchar) Name of the city 
    - c_state_code (varchar) State abbreviation
    - c_lat (varchar) Latitude
    - c_long (varchar) Longitude

Airports - Dimension table - Inserted from staging airports joined with cities to ensure cities are valid
- Columns
    - a_id (bigint) index column,
    - a_city_id (bigint), Foreign key reference to city
    - a_iata_code (varchar) - IATA code
    - a_type (varchar) - Airport type
    - a_name (varchar) - Name of airport
    - a_elevation_ft (float) - Elevation of airport

Temperatures - Dimension table - Temperatures inserted from staging data joined with city data to ensure valid city
- Columns
    - t_city_id   (bigint) - Foreign key reference to city
    - t_date      (varchar) - Date string 
    - t_month     (integer) - Month extracted from date
    - t_year      (integer) - Year extracted from date
    - t_avg_temp  (float) - Average temperature for the date
    - t_avg_temp_uncertainty (float) - Average temperature uncertainty
    - t_average_temp_month (float) - Average temperature across all years for the month.

Statistics - Dimension table - Statistics for the city selected from staging cities joined with city table to ensure 
- Columns
    - s_city_id   (bigint) - Foreign key reference to city
    - s_population (integer) - Total population
    - s_median_age  (float) - Median age for city
    - s_avg_household (float) - Average household size
    - s_cnt_male  (integer) - Male population
    - s_per_male   (float) - Percentage male of population
    - s_cnt_female  (integer) - Female population
    - s_per_female  (float) - Percentage female of population
    - s_cnt_veterans  (integer) - Number of veterans
    - s_per_veterans  (float) - Veteran percentage of population
    - s_cnt_foreign_born (integer) - Number of foreign born
    - s_per_foreign_born (float) - Percentage of population foreign born
    - s_cnt_white   (integer) - Number of race white
    - s_per_white   (float) - Percentage of race white
    - s_cnt_his_latino  (integer) - Number of race hispanic latino
    - s_per_his_latino  (float) - Percentage of race hispanic latino
    - s_cnt_asian  (integer) - Number of race asian
    - s_per_asian  (float) - Percentage of race asian
    - s_cnt_amer_ind_ak_native (integer) - Number of race american indian or alaska native
    - s_per_amer_ind_ak_native (float) - Percent of race american indian or alaska native
    - s_cnt_black   (integer) - Number of race black or african american
    - s_per_black_afr_amer  (float) - Percentage of race black or african american

Travelers - Fact table - Data inserted from staging travelers data joined with airports to ensure only travel data is loaded for which we have an airport and city.
- Columns
    - p_id (bigint) index column
    - p_airport_id  (integer) foreign key to airport id
    - p_age  (integer) - Age of traveler
    - p_visa_code  (integer) - Visa code of traveler
    - p_gender  (varchar) - Gender of traveler
    - p_year_of_birth  (integer) - Year of birth
    - p_arrival_year  (integer) - Arrival year
    - p_arrival_month  (integer) - Arrival month
    - p_arrival_day  (integer) - Arrival day

Most analytic queries would be initially started against the songplays table with joins being placed against the other dimension tables for information about the song, artist, user or time that it was played. This is why the songplays table is considered the fact table and the other tables are considered dimensions building out a star schema. 

Some issues with the data is that once filtering airports with cities and thus further with statistics there are far fewer useful cities with statistics then one might think so any results there may not included all travelers when joining with statistics. 

# ETL pipeline

The pipeline is designed as such to load the data from files into Pandas or Spark then transform it by removing duplicates and filtering out unnecessary data before writing out files in an alternate location to be loaded into Redshift and manipulated further.

From Redshift the analytics queries could be run to analyze patterns of travelers based on their destinations.

# How to use

1) Edit config.cfg to set parameters for Redshift, AWS, and all file paths
    a) Take note that the TRAVELERS output path should containing a trailing slash
    b) Also I used a single file for importing the travelers information. Alternate files could be used but schema may be slightly different.
    c) Your IAM role should be capable of S3 read access and added to your cluster permissions
    d) Your AWS KEY/SECRET should be for a user that has access to S3 and Redshift. Full access may make this simpler as that is what I used.
2) Run `python dataprep.py` - This will manipulate the data in CSV format to be ready to load into Redshift
    a) See troubleshooting section if you receive a pandas NamedAgg error.
3) Run `python create_tables.py` - This will drop and create all the necessary data tables in Redshift
4) Run `python etl.py` - This will load the data into Redshift via staging tables and then extract and load the data into the fact and dimension schema
 
# Troubleshooting
If you receive an error such as `Unexpected error running program: module 'pandas' has no attribute 'NamedAgg'` when running the dataprep.py in the workspace then you may need to do the following:
`pip3 install --upgrade pandas`

Also if you run it multiple times you should make sure that you choose a different bucket folder or remove the files from your bucket since the travelers data files will have different names each time and will thus all be imported causing many duplicates. 
 
# Addressing Other Scenarios

1) The data was increased by 100x.

Assuming the data increased by this much the use of Spark may be more necessary for the other dataframes and potentially loading the data first directly into redshift before doing the processing with Spark and then writing it out to the staging tables before finally loading it into the final data model tables. 
Ideally if the data increased but it was split up into hourly or daily files such that the processing could be happening on a fixed schedule by building a more complete data pipeline with Apache Airflow. 


2) The pipelines would be run on a daily basis by 7 am every day.

In this situation we would set up a cron schedule for either 5am or something earlier depending on how long it takes to run. This cron could be applied to a data pipeline composed of the code in these scripts. Basically running through the code in dataprep.py first and then etl.py but breaking down the pieces within each file to separate tasks in the DAG. This would be done through Apache airflow so that files that needed to be processed daily could be while possibly other pieces of the pipeline might not need to be run daily. 

3) The database needed to be accessed by 100+ people.

Redshift is a good final destination for our FACT and dimension tables such that hundreds or thousands of team members can run queries on the data and attach analytics tools to the database. 

# Recommendations for finding some insights within the data

An example of how the data could be used would be a query like the following:

Tell me what the average age of all travelers to an airport in a city and order it by descending median_age of the city.
`select avg_age, s_median_age, c_name, c_state_code from 
(select avg(p_age) as avg_age, a_city_id as age_city_id from travelers 
join airports on p_airport_id = a_id
join city on a_city_id = c_id
group by a_city_id) as averages
join city on c_id = age_city_id
join statistics on s_city_id = c_id
order by s_median_age desc`

Another example would be something like the following:

Give me the city destination with the highest number of travelers where that city has the highest percent of foreign born population.

`select c_name, c_state_code, s_per_foreign_born, cnt from statistics join (
select c_id, count(*) as cnt from travelers 
join airports on p_airport_id = a_id
join city on a_city_id = c_id
join statistics on s_city_id = c_id
group by c_id
order by cnt desc) as t
on t.c_id = s_city_id
join city as c on s_city_id = c.c_id
order by s_per_foreign_born desc`

Lastly another one that includes query from the weather temperature data:

Can we conclude that people wanted to travel to places typically warmer than other places?

`select c_name, c_state_code, t_average_temp_month, cnt, round(100*(cnt::float/total::float),2) as percent_of_total_travelers
from temperatures 
join (
select c_id, count(*) as cnt from travelers 
join airports on p_airport_id = a_id
join city on a_city_id = c_id
join statistics on s_city_id = c_id
group by c_id
order by cnt desc) as t on t.c_id = t_city_id
join city as c on t_city_id = c.c_id
cross join (select count(*) as total
  from travelers
)
where t_year = '2013' and t_month = '4'
order by percent_of_total_travelers desc`


# Disclaimer

Some of the input data could not be included due to the size of the source files

Those files are:
GlobalLandTemperaturesByCity.csv
- This could be downloaded in places like this:
https://www.kaggle.com/lajonutyte/globallandtemperaturesbycity/data

18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat
- A sample of the data in this file can be found in input_data/immigration_data_sample.csv


