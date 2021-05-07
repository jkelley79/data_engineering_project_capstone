import pandas as pd
import datetime
import pyspark.sql.types as T
import pyspark.sql.functions as F
import configparser
from pyspark.sql import SparkSession
import os
import glob
import boto3

def prep_cities_data(config):
    """
    Read cities data in from CSV and format into appropriate dataframe before exporting to CSV again
    """
    # Enumerate the race codes to populate new columns
    races = ['White', 'Hispanic or Latino', 'Asian', 'American Indian and Alaska Native', 'Black or African-American']
    
    # Read in the CSV and then sort by state, city
    citiesdf = pd.read_csv(config['INPUT']['CITIES'], sep=';')
    citiesdf = citiesdf.sort_values(by=['State','City'])
    
    # Split out race and counts into separate dataframe
    citiesracesdf = citiesdf[['City', 'State','Race','Count']]
    
    mergedcities = citiesdf
    for race in races:
        # Merge a count and percent column for each race into the main dataframe
        racedf = citiesracesdf[citiesracesdf['Race'] == f"{race}"]
        racedf = racedf.rename(columns = {'Count': f"{race} Count"}, inplace=False)
        racedf = racedf.drop(['Race'], axis=1)
        mergedcities = mergedcities.merge(racedf, on=['City', 'State'])
        mergedcities[f"Percent {race}"] = mergedcities[f"{race} Count"] / mergedcities['Total Population']
    
    # Merge percentages of population for various existing count columns
    otherstats = ['Male Population', 'Female Population', 'Number of Veterans', 'Foreign-born']
    for stat in otherstats:
        mergedcities[f"Percent {stat}"] = mergedcities[f"{stat}"] / mergedcities['Total Population']

    # Drop any duplicate records of City State since there will be for each race
    final_cities = mergedcities.drop_duplicates(subset=["City", "State"]).sort_values(by=['City'])
    
    # Drop additional unused columns
    final_cities = final_cities.drop(columns=['State', 'Race','Count'])

    # Convert column types 
    convert_dict = {'City': str, 
                'Male Population': int,
                'Female Population': int,
                'Total Population': int,
                'Number of Veterans': int,
                'Foreign-born': int,
                'State Code': str
               } 
    final_cities = final_cities.astype(convert_dict) 

    # Round percentages to two decimals
    final_cities = final_cities.round({'Percent White': 2, 
                                             'Percent Asian': 2,
                                             'Percent American Indian and Alaska Native': 2, 
                                             'Percent Black or African-American': 2,
                                             'Percent Hispanic or Latino':2,
                                             'Percent Male Population':2,
                                             'Percent Female Population':2,
                                             'Percent Number of Veterans':2,
                                             'Percent Foreign-born':2
                                            })

    # Rename the columns
    final_cities = final_cities.rename(columns={"City": "city",
                   "State Code": "state",
                   "Median Age": "median_age",
                   "Average Household Size": "avg_household",
                   "Male Population": "cnt_male",
                   'Percent Male Population': 'per_male',
                   "Female Population": "cnt_female",
                   'Percent Female Population':'per_female',
                   "Total Population": "population",
                   "Number of Veterans": "cnt_veterans",
                   'Percent Number of Veterans':"per_veterans",
                   "Foreign-born":"cnt_foreign_born",
                   'Percent Foreign-born':"per_foreign_born",
                   "White Count": "cnt_white",
                   "Percent White": "per_white",
                   "Asian Count": "cnt_asian",
                   'Percent Asian': "per_asian",
                   "American Indian and Alaska Native Count": "cnt_amer_ind_ak_native",
                   'Percent American Indian and Alaska Native': "per_amer_ind_ak_native",
                   "Black or African-American Count": "cnt_black",
                   'Percent Black or African-American': "per_black_afr_amer",
                   "Hispanic or Latino Count":"cnt_his_latino",
                   'Percent Hispanic or Latino':"per_his_latino",
                  })

    # Output to CSV
    final_cities.to_csv(config['OUTPUT']['FOLDER'] + '/' + config['OUTPUT']['CITIES'],index=False)

def prep_airport_data(config):
    """
    Read airport data in from CSV and format into appropriate dataframe before exporting to CSV again
    """
    # Read in data from csv
    airportcodes = pd.read_csv(config['INPUT']['AIRPORTS'])

    # Drop null IATA code columns and filter additional bad values
    majorairports = airportcodes[airportcodes.iata_code.notnull()]
    majorairports = majorairports[majorairports.iata_code != '0']
    majorairports = majorairports[majorairports.iata_code != '-']
    majorairports.sort_values(by='iata_code')

    # Reduce the number of columns
    clean_airports = majorairports.filter(['iata_code', 'type', 'name', 'elevation_ft', 'continent', 'iso_country', 'iso_region', 'municipality','coordinates'], axis=1)

    # Convert coordinates to latitude and longitude separate columns
    clean_airports[['long', 'lat']] = clean_airports["coordinates"].str.split(pat=",", expand=True)
    clean_airports = clean_airports.drop('coordinates', axis=1)
    
    # Drop any non-US based data from airports
    clean_airports['iso_country'] = clean_airports['iso_country'].astype(str)
    clean_airports = clean_airports[(clean_airports['iso_country']=="US")]

    # Split up the data for iso_region to get state values
    clean_airports[['country', 'state']] = clean_airports["iso_region"].str.split(pat="-", expand=True)
    
    # Drop unused columns and rename others
    clean_airports = clean_airports.drop(columns=['continent','iso_country','iso_region','country'])
    clean_airports = clean_airports.rename(columns={"municipality": "city"})
    clean_airports = clean_airports.sort_values(by=['city'], ascending=False)

    # Convert the data in the columns to specific types
    convert_dict = {'iata_code': str, 
                    'type': str,
                    'name': str,
                    'city': str,
                    'lat': float,
                    'long': float,
                    'state': str
                } 
    final_airports = clean_airports.astype(convert_dict)
    
    # Round data appropriately
    final_airports = final_airports.round({'lat': 2,'long':2})
    
    # Format latitude/longitude into relevant N/S or E/W rather than negative numbers
    final_airports["long"] = final_airports.apply(lambda x: f"{abs(x['long'])}W" if x['long'] < 0 else f"{x['long']}E", axis=1)
    final_airports["lat"] = final_airports.apply(lambda x: f"{abs(x['lat'])}S" if x['lat'] < 0 else f"{x['lat']}N", axis=1)

    # Output final data frame to CSV
    final_airports.to_csv(config['OUTPUT']['FOLDER'] + '/' + config['OUTPUT']['AIRPORTS'],index=False)

def prep_temperature_data(config):
    """
    Read temperature data in from CSV and format into appropriate dataframe before exporting to CSV again
    """
    # Read in CSV file
    temperaturedf = pd.read_csv(config['INPUT']['TEMPERATURES'])
    temperaturedf = temperaturedf.sort_values(by=['dt'], ascending=False)

    # Split out data columns for month and year 
    temperaturedf['month'] = pd.DatetimeIndex(temperaturedf['dt']).month
    temperaturedf['year'] = pd.DatetimeIndex(temperaturedf['dt']).year
    temperaturedf = temperaturedf.sort_values(by=['dt','City'], ascending=False)

    # Drop any rows with empty columns
    temperature_clean = temperaturedf.dropna()

    # Drop data to only include US
    temperature_clean = temperature_clean[(temperature_clean['Country']=="United States")]
    
    # Drop extra columns and rename columns
    final_temps = temperature_clean.drop(columns=['Country'])
    final_temps = final_temps.rename(columns={
        "dt": "date",
        "AverageTemperature": "avg_temp",
        "AverageTemperatureUncertainty": "avg_temp_uncertainty",
        "City": "city",
        "Latitude": "lat",
        "Longitude": "long"
    })
    
    # Calculate the average month temperature across years by city
    temp_averages = final_temps.groupby(['city', 'month'], as_index=False).agg(average_temp_month=pd.NamedAgg(column="avg_temp",aggfunc="mean"))
    
    # Combine the results with the main dataframe and round values
    combined_temps = pd.merge(final_temps, temp_averages, how='left', left_on=['city','month'], right_on = ['city','month'])
    combined_temps = combined_temps.round({'avg_temp': 2, 
                                        'avg_temp_uncertainty': 2,
                                        'average_temp_month': 2
                                        })
    # Output the data back to CSV 
    combined_temps.to_csv(config['OUTPUT']['FOLDER'] + '/' + config['OUTPUT']['TEMPERATURES'],index=False)

def prep_travelers_data(config):
    """
    Read travelers data in from SAS files into Spark and export to CSV
    """
    # Initiate spark connection
    spark = SparkSession.builder.config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11")\
        .enableHiveSupport().getOrCreate()

    # Read data file into spark dataframe
    i94_df = spark.read.format('com.github.saurfang.sas.spark').load(config['INPUT']['TRAVELERS'])
    
    # Rename columns
    travel_data = i94_df.selectExpr("i94port as iata_code", "arrdate as arrival_date","i94bir as age","i94visa as visa","biryear as year_of_birth","gender")
    
    # Filter out any non-existant airport codes
    travel_data = travel_data.filter(travel_data.iata_code != 'XXX')

    # Convert the SAS date to a regular date type
    start_date = datetime.datetime(1960, 1, 1)
    convert_sas_date = F.udf(lambda x: start_date + datetime.timedelta(days = int(x)) if x is not None else None, T.DateType())
    travel_data_clean = travel_data.withColumn('arrival_date', convert_sas_date('arrival_date'))

    # Extract the arrival year, month, and day into separate columns
    travel_data_clean = travel_data_clean.withColumn("arrival_year", F.date_format(F.col("arrival_date"), "y"))
    travel_data_clean = travel_data_clean.withColumn("arrival_month", F.date_format(F.col("arrival_date"), "M"))
    travel_data_clean = travel_data_clean.withColumn("arrival_day", F.date_format(F.col("arrival_date"), "d"))

    # Drop additional column and filter out nulls from gender 
    travel_data_clean = travel_data_clean.drop(F.col('arrival_date'))
    travel_data_clean = travel_data_clean.filter(travel_data_clean.gender.isNotNull())

    # Cast datatypes to the appropriate column types
    travel_data_final = travel_data_clean.selectExpr("iata_code", "cast(age as int) as age", "cast(visa as int) as visa","gender","cast(year_of_birth as int) as year_of_birth", "cast(arrival_year as int) as arrival_year", "cast(arrival_month as int) as arrival_month", "cast(arrival_day as int) as arrival_day")

    # Export the dataframe to csv format
    travel_data_final.write.mode("overwrite").csv(config['OUTPUT']['FOLDER'] + '/' + config['OUTPUT']['TRAVELERS'])

    # Remove files that are not necessary for import to redshift
    for f in os.listdir(config['OUTPUT']['FOLDER'] + '/' + config['OUTPUT']['TRAVELERS']):
        if f.endswith('crc') or f.startswith('_'):
            os.remove(f"{config['OUTPUT']['FOLDER'] + '/' + config['OUTPUT']['TRAVELERS']}/{f}")

def upload_to_s3(config):
    """
    Upload the data files to S3 to be loaded into Redshift
    """
    s3 = boto3.resource('s3',
                       region_name=config['AWS']['REGION'],
                       aws_access_key_id=config['AWS']['KEY'],
                       aws_secret_access_key=config['AWS']['SECRET']
                    )
    s3.Bucket(config['S3']['BUCKET']).upload_file(config['OUTPUT']['FOLDER'] + '/' + config['OUTPUT']['CITIES'],config['S3']['FOLDER'] + '/' + config['OUTPUT']['CITIES'])
    s3.Bucket(config['S3']['BUCKET']).upload_file(config['OUTPUT']['FOLDER'] + '/' + config['OUTPUT']['AIRPORTS'],config['S3']['FOLDER'] + '/' + config['OUTPUT']['AIRPORTS'])
    s3.Bucket(config['S3']['BUCKET']).upload_file(config['OUTPUT']['FOLDER'] + '/' + config['OUTPUT']['TEMPERATURES'],config['S3']['FOLDER'] + '/' + config['OUTPUT']['TEMPERATURES'])
    for f in os.listdir(config['OUTPUT']['FOLDER'] + '/' + config['OUTPUT']['TRAVELERS']):
        if f.endswith('csv'):
            # Since travelers is a folder we don't need the trailing slash between the folder and the file name
            s3.Bucket(config['S3']['BUCKET']).upload_file(config['OUTPUT']['FOLDER'] + '/' + config['OUTPUT']['TRAVELERS'] + f, config['S3']['FOLDER'] + '/' + config['OUTPUT']['TRAVELERS'] + f)
            
def main():
    """
    Main program entry point to load file data into dataframes, manipulate and write back out into files for staging import
    """
    try:
        config = configparser.ConfigParser()
        config.read('config.cfg')
        
        print ('######## PREP CITY DATA ###########')
        prep_cities_data(config)

        print ('######## PREP AIRPORT DATA ###########')
        prep_airport_data(config)

        print ('######## PREP TEMPERATURE DATA ###########')
        prep_temperature_data(config)

        print ('######## PREP TRAVELERS DATA ###########')
        prep_travelers_data(config)
        
        print ('######## UPLOAD TO S3 ###########')
        upload_to_s3(config)

    except Exception as exc:
        print('Unexpected error running program: {}'.format(exc))

if __name__ == "__main__":
    main()