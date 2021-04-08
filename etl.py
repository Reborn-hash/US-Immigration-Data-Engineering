import pandas as pd
import os
from datetime import datetime, timedelta
from pyspark.sql import types as T
from pyspark.sql.functions import col, split, udf
from pyspark.sql.types import StructType as R, StructField as Fld, StringType as Str
from pyspark.sql import SparkSession
import boto3
import configparser

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']
AWS_KEY = config.get('AWS','AWS_ACCESS_KEY_ID')
AWS_SECRET = config.get('AWS','AWS_SECRET_ACCESS_KEY')
AWS_S3_BUCKET = config.get('AWS','S3')

def create_spark_session():
    """
    Creates and returns spark session.
    """
    
    spark = SparkSession.builder.\
    config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11")\
    .enableHiveSupport().getOrCreate()
    
    return spark

def clean_mappings(file_name):
    """
    Function to clean various mappings data, including:
        - countries.txt
        - ports.txt
        - state_codes.txt
        - travel_mode.txt
        - visa.txt
    
    Input: 
        file name without extension
    
    Returns:
        Cleaned dataframe
    """
    df = pd.read_csv("sas_labels/" + file_name + '.txt', sep='=', header=None)
    df.columns = ['code', file_name]
    
    df.iloc[:,1] = [n.strip().replace("\'","").replace(";","") for n in df.iloc[:,1]]
    
    if file_name == 'country':
        col = []
        for n in df.iloc[:,1]:
            if 'no country' in n.lower() or 'invalid' in n.lower() or 'collapsed' in n.lower():
                col.append("Other")
            else:
                col.append(n)
        df.iloc[:,1] = col
        
    elif file_name in ['state_codes', 'ports']:
        df.iloc[:,0] = [n.strip().replace("\'","").replace(";","").replace("\t","") for n in df.iloc[:,0]]   
        
    if file_name == 'ports':
        split = df['ports'].str.split(", ", n = 1, expand = True)
        df['ports'], df['State'] = split[0], split[1]
    
    # Data quality checks
    print(file_name+":", df.count())
    assert len(df) > 0
    return df

def clean_cities(cities_df):
    """
    Function to process and clean city data.
    
    Input:
        Dataframe of us-cities demographics
    
    Returns:
        cleaned cities dataframe
    """
    
    cities_df.columns = ['city',
                         'state',
                         'median_age',
                         'male_population',
                         'female_population',
                         'total_population',
                         'number_of_veterans',
                         'foreign_born',
                         'average_household_size',
                         'state_code',
                         'race',
                         'count']
    
    # Data quality checks
    print("cities_df:", cities_df.count())
    assert len(cities_df) > 0
    return cities_df

def clean_airports(spark, df_airports):
    """
    Function to process and clean airport data.
    
    Input: 
        df_airports: df containing airport data 
    
    Returns: 
        Cleaned airport Spark dataframe
    """
    
    df_airports['us_state'] = df_airports['iso_region'].str.split("-", n = 1, expand = True)[1]
    coord_df = df_airports['coordinates'].str.split(", ", n = 1, expand = True)
    df_airports['lattitude'] = coord_df[0]
    df_airports['longitude'] = coord_df[1]
    df_airports.drop(['coordinates','iso_region'],axis=1,inplace=True)
    
    print("df_airports:", df_airports.count())
    
    schema = R([    
        Fld("airport_id",Str()),
        Fld("type",Str()),
        Fld("name",Str()),
        Fld("elevation_ft",Str()),
        Fld("continent",Str()),
        Fld("iso_country",Str()),
        Fld("municipality",Str()),
        Fld("gps_code",Str()),
        Fld("iata_code",Str()),
        Fld("local_code",Str()),
        Fld("us_state",Str()),
        Fld("lattitude",Str()),
        Fld("longitude",Str())
    ])
    spark_airports = spark.createDataFrame(df_airports, schema=schema)
    
    # Data quality check
    print("spark_airports:", spark_airports.count())
    return spark_airports

def process_immigration_data(spark, df_states, df_travel_mode, df_visa):
    
    """
    This function processes immigration data and joins the data with
    states, travel mode and visa (reason for travel) data.
    
    Inputs: 
        df_states: df containing us states and state codes
        df_travel_mode: df containing travel mode and codes
        df_visa: df containing reasons for travel and codes
        
    Returns: 
        Consolodated immigration dataframe
    """
    
    # udfs to clean dates
    def convert_datetime(x):
        try:
            start = datetime(1960, 1, 1)
            return start + timedelta(days=int(x))
        except:
            return None

    def convert_date_added(x):
        try:
            month = int(x[:2])
            day = int(x[2:4])
            year = int(x[4:])

            return datetime(year,month,day)
        except:
            return None
    
    def convert_date_added2(x):
        x = str(x)
        try:
            month = int(x[4:6])
            day = int(x[6:])
            year = int(x[:4])

            return datetime(year,month,day)
        except:
            return None
    
    udf_datetime_from_sas = udf(lambda x: convert_datetime(x), T.DateType())
    udf_date_added = udf(lambda x: convert_date_added(x), T.DateType())
    udf_date_added2 = udf(lambda x: convert_date_added2(x), T.DateType())
    
    df_spark = spark.read.format('com.github.saurfang.sas.spark').load('../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat')
    df_spark = df_spark \
                .withColumn("arrdate", udf_datetime_from_sas("arrdate")) \
                .withColumn("depdate", udf_datetime_from_sas("depdate")) \
                .withColumn("dtaddto", udf_date_added("dtaddto")) \
                .withColumn("dtadfile", udf_date_added2("dtadfile"))

    df_spark.createOrReplaceTempView("df_immigration")
    df_states.createOrReplaceTempView("df_states")
    df_travel_mode.createOrReplaceTempView("df_travel_mode")
    df_visa.createOrReplaceTempView("df_visa")

    df_immigration = spark.sql(
    """SELECT 
            i94yr as year,
            i94mon as month,
            i94cit as country_of_birth,
            i94res as country_of_residence,
            i94port as port,
            arrdate as date_of_arrival,
            i94mode as mode_of_transport,
            i94addr as us_state,
            depdate as departure_date,
            i94bir as age_of_respondant,
            i94visa as reason_for_travel,
            dtadfile as date_added,
            visapost as visa_department,
            occup as occupation,
            entdepa as arrival_flag,
            entdepd as departure_flag,
            entdepu as update_flag,
            matflag as match_flag,
            biryear as year_of_birth,
            dtaddto as us_admission_date,
            gender,
            insnum as ins_number,
            airline,
            admnum as admission_number,
            fltno as flight_number,
            visatype as visa_type
    FROM df_immigration i
    JOIN df_states s ON i.i94addr = s.code
    JOIN df_visa v ON i.i94mode = v.code
    JOIN df_travel_mode t ON i.i94visa = t.code""")  
    
    # Data quality check
    print("df_immigration:", df_immigration.count())
    return df_immigration

def upload_files(path):
    """
    This function uploads all files in a directory to S3 whilst maintaining
    folder structure.
    """
    session = boto3.Session(
        aws_access_key_id=AWS_KEY,
        aws_secret_access_key=AWS_SECRET,
        region_name=AWS_S3_BUCKET
    )
    s3 = session.resource('s3')
    bucket = s3.Bucket('YOUR_BUCKET_NAME')
 
    for subdir, dirs, files in os.walk(path):
        for file in files:
            full_path = os.path.join(subdir, file)
            with open(full_path, 'rb') as data:
                bucket.put_object(Key=full_path[len(path)+1:], Body=data)

def main():
    """
    Instantiates dataframes and runs ETL.
    """
    print("Creating spark session...")
    spark = create_spark_session()
    airport_codes = pd.read_csv('airport-codes_csv.csv')
    cities = pd.read_csv('us-cities-demographics.csv',delimiter=';')
    
    file_out = 'DATA/'
    
    print("Cleaning data files...")
    df_ports = spark.createDataFrame(clean_mappings("ports")) 
    df_countries = spark.createDataFrame(clean_mappings("countries")) 
    df_state_codes = spark.createDataFrame(clean_mappings("state_codes"))     
    airport_codes = clean_airports(spark, airport_codes)
    cities = spark.createDataFrame(clean_cities(cities))
    
    print("Writing parquet files...")
    df_ports.write.mode("overwrite").parquet(file_out)
    df_countries.write.mode("overwrite").parquet(file_out)
    df_state_codes.write.mode("overwrite").parquet(file_out)
    airport_codes.repartitionByRange(3, "airport_id", "us_state").write.mode("overwrite").parquet(file_out)  
    cities.write.mode("overwrite").partitionBy("state").parquet(file_out)
    
    print("Processing immigration data...")
    df_visa = spark.createDataFrame(clean_mappings("visa")) 
    df_travel_mode = spark.createDataFrame(clean_mappings("travel_mode")) 
    im_data = process_immigration_data(spark, df_state_codes, df_travel_mode, df_visa)
    im_data.write.mode("overwrite").partitionBy("year", "month", "us_state").parquet(file_out)
    
    print("Uploading files to S3...")
    upload_files(file_out)
    
if __name__ == '__main__':
    main()