# Import Libraries
import pandas as pd
import seaborn as sns
import numpy as np
import matplotlib.pyplot as plt
import os
import glob
import psycopg2


#from sql_queries import *
import datetime as dt
import json
#from fuzzywuzzy import fuzz, process
import configparser
import time
from time import time
import boto3
from botocore.exceptions import ClientError
from random import random
import threading


###
import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql import SQLContext

from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, IntegerType, FloatType, TimestampType, LongType, DateType, NullType
#from pyspark.sql.types import *

import datetime 
from pyspark.sql.functions import year, month, dayofmonth, dayofweek, hour, minute, second, weekofyear
from pyspark.sql.functions import avg
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import udf, lit, datediff, when, col, date_format
from pyspark.sql.functions import isnan, when, count
from pyspark.sql import types as T
import pyspark.sql.functions as F
from pyspark.sql.types import *

import plotly.plotly as py
import plotly.graph_objs as go
import requests
requests.packages.urllib3.disable_warnings()



def create_spark_session():
    """
    Create spark session
  
    Returns: 
    spark object
    """            
    os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
    os.environ["PATH"] = "/opt/conda/bin:/opt/spark-2.4.3-bin-hadoop2.7/bin:/opt/conda/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/lib/jvm/java-8-openjdk-amd64/bin"
    os.environ["SPARK_HOME"] = "/opt/spark-2.4.3-bin-hadoop2.7"
    os.environ["HADOOP_HOME"] = "/opt/spark-2.4.3-bin-hadoop2.7"
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    df_spark =spark.read.load('./data/sas_data')
    
    return spark

def sas_program_file_value_parser(sas_source_file, value, columns):
    """Parses SAS Program file to return value as pandas dataframe
    Args:
        sas_source_file (str): SAS source code file.
        value (str): sas value to extract.
        columns (list): list of 2 containing column names.
    Return:
        None
    """
    file_string = ''
    
    with open(sas_source_file) as f:
        file_string = f.read()
    
    file_string = file_string[file_string.index(value):]
    file_string = file_string[:file_string.index(';')]
    
    line_list = file_string.split('\n')[1:]
    codes = []
    values = []
    
    for line in line_list:
        
        if '=' in line:
            code, val = line.split('=')
            code = code.strip()
            val = val.strip()

            if code[0] == "'":
                code = code[1:-1]

            if val[0] == "'":
                val = val[1:-1]

            codes.append(code)
            values.append(val)
        
            
    return pd.DataFrame(zip(codes,values), columns=columns)

def strip_all_columns(df):
    """
    Strip all columns in a dataframe
    
    Parameters:
    arg1 (dataframe)
    
    Returns:
    dataframe
    """
    for colu in df.columns:
        if df[colu].dtype == 'object':            
            mask = df[colu].notnull()
            df.loc[mask, colu] = df.loc[mask, colu].map(str.strip)            
    return df

def difference (list1, list2): 
    """
    Difference between two lists used to compare list of two sets of columns
    
    Parameters: 
    arg1 (list 1)
    arg2 (list 2)
    
    Returns: 
    list with difference
    
    Sample : print(difference(keep_columns, df_jan.columns))
    """                
    list_dif = [i for i in list1 + list2 if i not in list1 or i not in list2]
    return list_dif

def visualize_missing_values(df):
    """Given a dataframe df, visualize it's missing values by columns
    :param df:
    :return:
    """
    # lets explore missing values per column
    nulls_df = pd.DataFrame(data= df.isnull().sum(), columns=['values'])
    nulls_df = nulls_df.reset_index()
    nulls_df.columns = ['cols', 'values']

    # calculate % missing values
    nulls_df['% missing values'] = 100*nulls_df['values']/df.shape[0]

    plt.rcdefaults()
    plt.figure(figsize=(10,5))
    ax = sns.barplot(x="cols", y="% missing values", data=nulls_df)
    ax.set_ylim(0, 100)
    ax.set_xticklabels(ax.get_xticklabels(), rotation=90)
    plt.show()
    
def visualize_missing_values_spark(df):
    """Visualize missing values in a spark dataframe
    
    :param df: spark dataframe
    """
    # create a dataframe with missing values count per column
    nan_count_df = df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df.columns]).toPandas()
    
    # convert dataframe from wide format to long format
    nan_count_df = pd.melt(nan_count_df, var_name='cols', value_name='values')
    
    # count total records in df
    total = df.count()
    
    # now lets add % missing values column
    nan_count_df['% missing values'] = 100*nan_count_df['values']/total
    
    plt.rcdefaults()
    plt.figure(figsize=(10,5))
    ax = sns.barplot(x="cols", y="% missing values", data=nan_count_df)
    ax.set_ylim(0, 100)
    ax.set_xticklabels(ax.get_xticklabels(), rotation=90)
    plt.show()
    
def print_formatted_float(number):
    print('{:,}'.format(number))
        
def to_datetime(x):
    """
    Converts SAS date
  
    Parameters: 
    arg1 (days)    
  
    Returns: 
    date object or None
    """        
    try:
        start = dt.datetime(1960, 1, 1).date()
        return start + dt.timedelta(days=int(x))
    except:
        return None

udf_to_datetime_sas = udf(lambda x: to_datetime(x), DateType())


def to_datetimefrstr(x):
    """
    Converts date format
  
    Parameters: 
    arg1 (date)
  
    Returns: 
    date object or None
    """            
    try:
        return dt.datetime.strptime(x, '%m%d%Y')
    except:
        return None

udf_to_datetimefrstr = udf(lambda x: to_datetimefrstr(x), DateType())

def cdf_Ymd_to_mmddYYYY(x):
    """
    Converts date format
  
    Parameters: 
    arg1 (date)
  
    Returns: 
    date object or None
    """                
    try:
        return dt.datetime.strptime(x, '%Y%m%d')
    except:
        return None

udf_cdf_Ymd_to_mmddYYYY = udf(lambda x: cdf_Ymd_to_mmddYYYY(x), DateType())

def cdf_mdY_to_mmddYYYY(x):
    """
    Converts date format
  
    Parameters: 
    arg1 (date)
  
    Returns: 
    date object or None
    """                
    try:
        return dt.datetime.strptime(x, '%m%d%Y')
    except:
        return None

udf_cdf_mdY_to_mmddYYYY = udf(lambda x: cdf_mdY_to_mmddYYYY(x), DateType())

def process_immigration_df(spark, immigration_dfs1, PoE_df1, airport_codes_df1):
    """
    Process i94 dataframe
  
    Parameters: 
    arg1 (dataframe)
    arg2 (dataframe)
    arg3 (dataframe)
    arg4 (dataframe)
    arg5 (dataframe)
    arg6 (dataframe)
  
    Returns: 
    Processed dataframe
    """                    
    ps_start = time()
    print('{} : Starting to process immigration_df'.format(dt.datetime.now()))
     
    PoE_dfs1 = spark.createDataFrame(PoE_df1)

    airport_schema = StructType([
        StructField("type", StringType()),
        StructField("name", StringType()),
        StructField("elevation_ft", FloatType()),
        StructField("iso_country", StringType()),
        StructField("iso_region", StringType()),
        StructField("muncipality", StringType()),
        StructField("iata_code", StringType()),
        StructField("local_code", StringType()),
        StructField("coordinates", StringType()),   
        StructField("longitude", StringType()),   
        StructField("latitude", StringType()),   
        StructField("state", StringType()),   
    ])
    airport_codes_dfs1 = spark.createDataFrame(airport_codes_df1, airport_schema)

    # matflag is null
    immigration_dfs1 = immigration_dfs1.filter(immigration_df21.matflag.isNotNull())

    # visatype GMT  
    temp = immigration_df1.visatype.tolist()
    immigration_df1 = immigration_df1[immigration_df1.visatype.isin(temp)]

    # i94mode other than 1 2 3
    temp = [1, 2, 3]
    immigration_dfs1 = immigration_dfs1.filter(immigration_dfs1.i94mode.isin(temp) )

    # gender is null
    immigration_dfs1 = immigration_dfs1.filter(immigration_dfs1.gender.isNotNull())

    # Drop visatype GMT since it is an invalid visatype
    immigration_df1.drop(immigration_df1.index[immigration_df1['visatype'] == 'GMT'], inplace = True)

    # Dropping unused columns
    # Only below columns are used for analysis
    keep_columns = ['i94cit', 'i94res', 'i94port', 'arrdate', 'i94mode', 'depdate'
                    , 'i94bir', 'i94visa', 'biryear', 'gender', 'airline', 'admnum', 'fltno', 'visatype']
    drop_cols = difference(keep_columns, dfs_ids1.columns)
    #print(drop_cols)
    immigration_dfs1 = immigration_dfs1.drop(*drop_cols)

    # gender is null
    immigration_dfs1 = immigration_dfs1.filter(immigration_dfs1.gender.isNotNull())

    # Convert floats to ints
    cols_to_convert_float_to_string = ['i94cit', 'i94res', 'arrdate', 'i94mode', 'depdate', 'i94bir'
                                , 'i94visa', 'biryear', 'admnum']
    for colu in cols_to_convert_float_to_integer:    
        immigration_dfs1 = immigration_dfs1.na.fill(0, subset=[colu])
        immigration_dfs1 = immigration_dfs1.withColumn(colu, immigration_dfs1[colu].cast(IntegerType()))

    # Mapping : Codes to descriptive
    #temp = {'1' : 'Air', '2' : 'Sea', '3' : 'Land', '9' : 'Not reported'}
    temp = [["1", "Air"], ["2", "Sea"], ["3","Land"], ["9", "Not reported"]]
    i94mode = spark.sparkContext.parallelize(temp).toDF(["code", "arrival_mode"])
    immigration_dfs1 = immigration_dfs1.join(i94mode, immigration_dfs1.i94mode == i94mode.code).select(immigration_dfs1["*"], i94mode["arrival_mode"])

    temp = [["1", "Business"], ["2", "Pleasure"], ["3", "Student"]]
    i94visa = spark.sparkContext.parallelize(temp).toDF(["code", "visit_purpose"])
    immigration_dfs1 = immigration_dfs1.join(i94visa, immigration_dfs1.i94visa == i94visa.code).select(immigration_dfs1["*"], i94visa["visit_purpose"])


#    from pyspark.sql.functions import *
    # Conversion of SAS encoded dates(arrdate & depdate)
    immigration_dfs1 = immigration_dfs1.withColumn("arrival_dt", udf_to_datetime_sas(immigration_dfs1.arrdate))
    immigration_dfs1 = immigration_dfs1.withColumn("departure_dt", udf_to_datetime_sas(immigration_dfs1.depdate))

    immigration_dfs1 = immigration_dfs1.withColumn("DaysinUS", datediff("departure_dt", "arrival_dt"))

    # Below corrections are carried out due to above adding 1960-01-01
    immigration_dfs1 = immigration_dfs1.withColumn("arrival_dt",when(col("arrival_dt")=="1960-01-01",lit(None)).otherwise(col("arrival_dt")))
    immigration_dfs1 = immigration_dfs1.withColumn("departure_dt",when(col("departure_dt")=="1960-01-01",lit(None)).otherwise(col("departure_dt")))
    immigration_dfs1 = immigration_dfs1.withColumn("DaysinUS",when(col("arrival_dt").isNull(),lit(None)).otherwise(col("DaysinUS")))
    immigration_dfs1 = immigration_dfs1.withColumn("DaysinUS",when(col("departure_dt").isNull(),lit(None)).otherwise(col("DaysinUS")))
    
    # Columns Rename
    immigration_dfs1 = (immigration_dfs1
                .withColumnRenamed("i94bir",  "age")
                .withColumnRenamed("i94cit", "CoC")
                .withColumnRenamed("i94res", "CoR")
                .withColumnRenamed("i94port", "PoE"))

    # Gender X to O
    immigration_dfs1 = immigration_dfs1.withColumn("gender", when(col("gender")=="X", lit("O")).otherwise(col("gender")))
                        
    # Final Drop of unused columns
    drop_cols = ['i94mode', 'i94visa', 'arrdate', 'depdate']
    #print(drop_cols)
    immigration_dfs1 = immigration_dfs1.drop(*drop_cols)

    ps_et = time() - ps_start
    print("=== {} Total Elapsed time is {} sec\n".format('Process immigration_dfs', round(ps_et,2) ))    
    return immigration_dfs1

def process_PoE_df(PoE_df1):
    """
    Summary line. 
    Process PoE_df dataframe
  
    Parameters: 
    arg1 (dataframe)
  
    Returns: 
    Processed dataframe
    """                    
    ps_start = time()
    print('{} : Starting to process PoE_df'.format(dt.datetime.now()))    
    
    # PoE_df : Split one column into two
    PoE_df1["citystate"] = PoE_df1["citystate"].map(str.strip)

    PoE_df1[['city', 'state']] = PoE_df1['citystate'].str.rsplit(",",n=1, expand=True)
    PoE_df1 = strip_all_columns(PoE_df1)

    # PoE_df : Remove invalid rows & ports outside US
    PoE_df1 = strip_all_columns(PoE_df1).copy()
    PoE_df1 = PoE_df1[PoE_df1.state.notnull()]

    cond1 = PoE_df1.city.str.lower().str.contains('collapsed')
    PoE_df1 = PoE_df1[~cond1]

    cond1 = PoE_df1.city.str.lower().str.contains('no port')
    PoE_df1 = PoE_df1[~cond1]

    cond1 = PoE_df1.city.str.lower().str.contains('unknown')
    PoE_df1 = PoE_df1[~cond1]

    cond1 = PoE_df1.city.str.lower().str.contains('identifi')
    PoE_df1 = PoE_df1[~cond1]

    PoE_df1 = PoE_df1[PoE_df1.state.str.len() == 2]

    ps_et = time() - ps_start
    print("=== {} Total Elapsed time is {} sec\n".format('Process PoE_df', round(ps_et,2) ))      
    
    return PoE_df1

def process_airport_codes_df(process_airport_codes_df1):
    """
    Process process_airport_codes_df dataframe
  
    Parameters: 
    arg1 (dataframe)
    arg2 (dataframe)
  
    Returns: 
    Processed dataframe
    """                        
    ps_start = time()
    print('{} : Starting to process process_airport_codes_df'.format(dt.datetime.now()))
    #print('== process_airport_codes_df1 = ',process_airport_codes_df1.shape[0])
    
    # process_airport_codes_df : Keeping only US Airports 
    # Dropping unused columns continent, gps_code, ident
    drop_cols = ['continent', 'gps_code', 'ident']
    airport_codes_df1.drop(drop_cols, inplace = True, axis = 1)

    #Keep only US Airports
    airport_codes_df1 = airport_codes_df1[airport_codes_df1.iso_country =='US'].copy()

    # Eliminate rows which has iata code as null
    airport_codes_df1 = airport_codes_df1[~airport_codes_df1.iata_code.isnull()].copy()

    # Keep only airport which is of type small_airport, medium_airport & large_airport
    temp = ['small_airport', 'medium_airport', 'large_airport']
    airport_codes_df1 = airport_codes_df1[airport_codes_df1['type'].isin(temp)].copy()

    # Split coordinates into separate columns
    airport_codes_df1[['longitude','latitude']] = airport_codes_df1.coordinates.str.split(", ",expand=True)

    # Extract state code from iso_region
    airport_codes_df1['state'] = airport_codes_df1.iso_region.str.slice(start=3)

    # Removing rows having 'Duplicate' in airport names
    airport_codes_df1 = airport_codes_df1[~airport_codes_df1.name.str.contains('uplicate')]

    ps_et = time() - ps_start
    print("=== {} Total Elapsed time is {} sec\n".format('Process airport_codes_df', round(ps_et,2) ))      

    #print('== airport_codes_df1 = ',airport_codes_df1.shape[0])    
    return airport_codes_df1

def process_demographics_df(demographics_df1):
    """
    Process demographics_df dataframe
  
    Parameters: 
    arg1 (dataframe)
    arg2 (dataframe)
  
    Returns: 
    Processed dataframes (demographics_df1, demog_df, race_df)
    """                        
    
    ps_start = time()
    print('{} : Starting to process demographcis_df'.format(dt.datetime.now()))
    
    # Convert floats to ints
    cols_to_convert_float_to_int = ['Male Population', 'Female Population', 'Total Population', 'Number of Veterans', 'Foreign-born', 'Count']

    for col in cols_to_convert_float_to_int:
        demographics_df1[col] = demographics_df1[col].replace(np.nan, 0)
        demographics_df1[col] = demographics_df1[col].astype(int)

    # Split based on Population
    demog_df = demographics_df1[['State', 'City', 'Median Age', 'State Code', 'Male Population', 'Female Population', 'Total Population', 'Number of Veterans', 'Foreign-born']]
    demog_df = demog_df.drop_duplicates()

    # Split based on Race
    race_df = demographics_df1[['State', 'City', 'Race', 'Count']].copy()

    # Summing up the duplicates
    race_df = race_df.groupby(['State', 'City', 'Race']).sum().reset_index().copy()

    # Unmelting to converts Race rows to columns
    race_df = race_df.set_index(['State','City','Race']).Count.unstack().reset_index()
    race_df.columns.name = None
    race_df[race_df.State =='Alabama'].head()

    # Convert floats to ints
    cols_to_convert_float_to_int = ['American Indian and Alaska Native','Asian', 'Black or African-American', 'Hispanic or Latino', 'White']

    for col in cols_to_convert_float_to_int:
        race_df[col] = race_df[col].replace(np.nan, 0)
        race_df[col] = race_df[col].astype(int)

    # Rename column names
    race_df.rename(columns={"American Indian and Alaska Native": "American_Indian_and_Alaska_Native"
                        , "Black or African-American":"Black_or_African_American"
                        , "Hispanic or Latino":"Hispanic_or_Latino"}, inplace=True)

    demog_df.rename(columns={"Median Age": "Median_Age" , "State Code":"State_Code", "Male_Population":"Male_Population"
                         , "Female Population":"Female_Population", "Total Population":"Total_Population" 
                         , "Number of Veterans":"Number_of_Veterans", "Foreign-born" : "Foreign_born"}, inplace=True)

    ps_et = time() - ps_start
    print("=== {} Total Elapsed time is {} sec\n".format('Process demographics_df', round(ps_et,2) ))      
    
    return demographics_df1, race_df, demog_df

def process_worldtemp_df(worldtemp_df1):
    """
    Process worldtemp_df dataframe
  
    Parameters: 
    arg1 (dataframe)
  
    Returns: 
    Processed dataframe
    """                        

    ps_start = time()
    print('{} : Starting to process worldtemp_df'.format(dt.datetime.now()))
    
    # Keeping only USA
    worldtemp_df1 = worldtemp_df1[worldtemp_df1.Country=='United States'].copy()

    # Rounding temperature to decimals=3 
    worldtemp_df1['AverageTemperature'] = worldtemp_df1['AverageTemperature'].round(decimals=3)

    # Dropping unused columns
    drop_cols = ['AverageTemperatureUncertainty', 'Latitude', 'Longitude']
    worldtemp_df1.drop(drop_cols, inplace = True, axis = 1)

    # Removing missing temperatures
    worldtemp_df1 = worldtemp_df1[~(worldtemp_df1.AverageTemperature.isnull())]

    # Eliminating the duplicates(ie., multiple locations in same city)
    worldtemp_df1 = worldtemp_df1.drop_duplicates(['dt', 'City', 'Country'],keep= 'first')

    # Convert dt to datetime from object
    worldtemp_df1['dt'] = pd.to_datetime(worldtemp_df1.dt)

    ps_et = time() - ps_start
    print("=== {} Total Elapsed time is {} sec\n".format('Process worldtemp_df', round(ps_et,2) ))
    
    return worldtemp_df1

def main():   
    total_start = time()
    spark = create_spark_session()

    ##### Read datasets ##### 
    print('{} : Starting to read datasets'.format(dt.datetime.now()))
    # JSON to dictionary
    fname = './data/i94port.json'
    i94port = json.load(open(fname))

    # Airport Code Table
    airport_codes_df = pd.read_csv('./data/airport-codes_csv.csv')

    # U.S. City Demographic Data
    demographics_df = pd.read_csv('./data/us-cities-demographics.csv', sep=';')

    # World Temperature Data
    worldtemp_df = pd.read_csv('./data/world_temperature.csv')

    ##### Create copies of dataframe #####
    immigration_df1 = immigration_df.copy()
    PoE_df1 = PoE_df.copy()
    airport_codes_df1 = airport_codes_df.copy()
    demographics_df1 = demographics_df.copy()
    worldtemp_df1 = worldtemp_df.copy()

    # Running strip() on all string columns
    immigration_df1 = strip_all_columns(immigration_df1).copy()
    PoE_df1 =strip_all_columns(PoE_df).copy()
    worldtemp_df1 = strip_all_columns(worldtemp_df1).copy()
   
    ##### Process dataframes ##### 
    PoE_df1 = process_PoE_df(PoE_df1)
    assert PoE_df1.shape[0] != 0,"PoE_df1 is empty"

    airport_codes_df1 = process_airport_codes_df(airport_codes_df1)
    assert airport_codes_df1.shape[0] != 0,"airport_codes_df1 is empty"

    print('Starting spark read of SAS datasets')
    data_dir = "../../data/18-83510-I94-Data-2016/"
    files = os.listdir(data_dir)
    sas_raw_datasets = ['../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat', '../../data/18-83510-I94-Data-2016/i94_sep16_sub.sas7bdat']
    flag=True
    for sasd in sas_raw_datasets:
        print(' ==> {} : Processing sas dataset {}'.format(dt.datetime.now(), sasd))
        temp =spark.read.format('./data/sas_data.spark').load(sasd)   
        assert temp.count() != 0,"immigration_dfs1 is empty"   
        if flag:
            immigration_dfs1 = process_ids(spark, temp, PoE_df1, airport_df1)
            flag=False
        else:
            immigration_dfs1 = immigration_dfs1.union(process_ids(spark, temp, PoE_df1, airport_df1))
    
    assert immigration_dfs1.count() != 0,"immigration_dfs1 is empty"   
    
    immigration_dfs1 =spark.read.format('com.github.saurfang.sas.spark').load('../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat')
    assert immigration_dfs1.count() != 0,"immigration_dfs1 is empty"
    immigration_dfs1 = process_ids(spark, immigration_dfs1, PoE_df1, airport_codes_df1)
    assert immigration_dfs1.count() != 0,"immigration_dfs1 is empty"
 
    demographics_df1, demog_df, race_df = process_demographics_df(demographics_df1)    
    assert demographics_df1.shape[0] != 0,"demographics_df1 is empty"
    assert demog_df.shape[0] != 0,"demog_df is empty"
    assert race_df.shape[0] != 0,"race_df is empty"

    worldtemp_df1 = process_worldtemp_df(worldtemp_df1)
    assert worldtemp_df1.shape[0] != 0,"worldtemp_df1 is empty"
    
    ##### Rows Count ##### 
    print("{} : Cleaned dataframes rows count".format(dt.datetime.now()))
    print("{:15} = {}".format("airport_codes_df1", airport_codes_df1.shape[0]))
    print("{:15} = {}".format("PoE_df1", PoE_df1.shape[0]))
    
    # Full dataframe is 28217609
    # print("{:15} = {}".format("immigration_dfs1", immigration_dfs1.count()))    
  
    print("{:15} = {}".format("demographics_df1", demographics_df1.shape[0]))
    print("{:15} = {}".format("demog_df", demog_df.shape[0]))
    print("{:15} = {}".format("race_df", race_df.shape[0]))
    print("{:15} = {}".format("worldtemp_df1", worldtemp_df1.shape[0]))
    
    ##### Dataframe writes #####     
    # Cleaned datasets
    print("{} : Write non-i94 dataframes(CSV)".format(dt.datetime.now()))
    
    # Create folder if it doesn't exist
    folder = "./outputs/"
    os.makedirs(os.path.dirname(folder), exist_ok=True)    
    
    #immigration_df1.to_csv('outputs/immigration_df1.csv', encoding='utf-8', index=False)
    airport_codes_df1.to_csv('outputs/airport_codes_df1.csv', encoding='utf-8', index=False)
    PoE_df1.to_csv('outputs/PoE_df1.csv', encoding='utf-8', index=False)
    demographics_df1.to_csv('outputs/demographics_df1.csv', encoding='utf-8', index=False)
    worldtemp_df1.to_csv('outputs/worldtemp_df1.csv', encoding='utf-8', index=False)
    demog_df.to_csv('outputs/demog_df.csv', encoding='utf-8', index=False)
    race_df.to_csv('outputs/race_df.csv', encoding='utf-8', index=False)

    print("{} : Write i94 dataframes(gzip)".format(dt.datetime.now()))
    immigration_dfs1.write\
      .format("com.databricks.spark.csv")\
      .option("header","true")\
      .option("codec", "org.apache.hadoop.io.compress.GzipCodec")\
      .save('./outputs-gzip/dfs_ids1.gzip')    
    
    total_et = time() - total_start
    print("=== {} Total Elapsed time is {} sec\n".format('Main()', round(total_et,2) ))
    print('{} : Done!'.format(dt.datetime.now()))
    
def s3_create_bucket(s3c, s3r, location, bucket_name):
    """
    Creates an S3 bucket, if it does not exist
  
    Parameters: 
    arg1 (S3 Client)
    arg2 (S3 Resource)
    arg3 (AWS Region)
    arg4 (Bucket name)
  
    Returns: None
    """     

    # Check if S3 bucket exists
    if( s3r.Bucket(bucket_name) in s3r.buckets.all() ):
        print('{} : {} {} {}'.format('S3', 'bucket', bucket_name, 'exists'))
    else:
        s3c.create_bucket(Bucket=bucket_name,
                         CreateBucketConfiguration=location)
        print('{} : {} {} {}'.format('S3', 'bucket', bucket_name, 'created'))

def s3_list_buckets(s3c):
    """
    Displays S3 Buckets
  
    Parameters: 
    arg1 (S3 Client)
  
    Returns: None
    """     

#    for bucket in s3r.buckets.all():
#         print(bucket)
    try:
        # Call S3 to list current buckets
        response = s3c.list_buckets()
        print('Total Buckets = ',len(response['Buckets']))
        for num, bucket in enumerate(response['Buckets'], start=1):
        #for bucket in response['Buckets']:
            print ('{}. {}'.format(num, bucket['Name']))
    except ClientError as e:
        print("The bucket does not exist, choose how to deal with it or raise the exception: "+e)
        return
        
def s3_delete_bucket(s3r, bucket_name):
    """
    Deletes S3 Bucket after emptying the bucket
  
    Parameters: 
    arg1 (S3 Resource)
    arg2 (Bucket name)
  
    Returns: None
    """     

    bucket = s3r.Bucket(bucket_name)
    
    bucket.objects.all().delete()    
    bucket.delete()    
    print('{} : {} {} {}'.format('S3', 'bucket', bucket_name, 'deleted'))


def local_get_all_files(folders):
    """
    Scans folder and prepares files list except folders starting with '.'
  
    Parameters: 
    arg1 (Folder names in array)
  
    Returns: 
    Return1 (Array of Selected files)
    Return2 (Array of Ignored files)
    """     
    
    selected_files, ignored_files = [], []    
    
    # 1. checking your current working directory
    print('Current Working Directory : ',os.getcwd())

    for folder in folders:
        # Get your current folder and subfolder event data
        filepath = os.getcwd() + '/' + folder
        print('Scanning Directory : ',filepath)

        # 2. Create a for loop to create a list of files and collect each filepath
        #    join the file path and roots with the subdirectories using glob
        #    get all files matching extension from directory

        for root, dirs, files in os.walk(filepath):
            files = glob.glob(os.path.join(root,'*.*'))
            #print('root = ',root)
            #print('dirs = ',dirs, ' : ',len(dirs))

            # Below condition is to ignore directories like ['.ipynb_checkpoints']
            dotdir = root.split('/')[-1]
            #print('dotdir = ',dotdir[0:1], 'length = ',len(dotdir))
            if( (dotdir[0:1]!='.' and len(dotdir) > 1) or (dotdir[0:1]=='.' and len(dotdir)==1) ):
                #print(files)
                for f in files :
                    selected_files.append(os.path.abspath(f))
            else:
                ignored_files.append(root)

    # 3. get total number of files found
    print('{} files found, {} files ignored'.format(len(selected_files), len(ignored_files) ))
    #print(all_files)
    return selected_files, ignored_files

def s3_upload_files(s3c, bucket_name, selected_files, rFindStr, rStr):
    """
    Upload only files to S3 will fail when a directory is encountered in the filepath
  
    Parameters: 
    arg1 (S3 Client)
    arg2 (Bucket name)
    arg3 (Selected files list)
    arg4 (Find string to replace)
    arg5 (String to be replaced with)
  
    Returns: None
    """     

    print('Uploading {} files to S3'.format(len(selected_files)))
    for f in selected_files:
        f = f.replace(rFindStr, rStr)
        # Uploads the given file using a managed uploader, which will split up large
        # files automatically and upload parts in parallel.    
        s3c.upload_file(f, bucket_name, f)

def s3_upload_parquet_files(s3, bucket_name, folder, path):
    """
    Uploads files/directories to S3
  
    Parameters: 
    arg1 : (S3 resource object)
    arg2 : (Bucket name)
    arg3 : (filename)
    arg4 : (local File path)
  
    Returns: None
    """        
    
    bucket = s3.Bucket(bucket_name)
    for subdir, dirs, files in os.walk(path):
        for file in files:
            full_path = os.path.join(subdir, file)
            with open(full_path, 'rb') as data:
                #print('Fullpath : ',full_path)
                #print(full_path[len(path)+1:])
                #print(folder+'/'+full_path[len(path)+1:])
                #bucket.upload_file('/tmp/' + filename, '<bucket-name>', 'folder/{}'.format(filename))
                bucket.put_object(Key=folder+'/'+full_path[len(path)+1:], Body=data)                

                
def delete_filetype_in_s3_bucket(s3c, bucket_name, delete_files, delete_types):
    """
    Delete specific files & file types from s3 bucket
  
    Parameters: 
    arg1 : (S3 client object)
    arg2 : (Bucket name)
    arg3 : (array of files to be deleted)
    arg4 : (array of file types)
  
    Returns: None
    """        
    
    try:
        response = s3c.list_objects_v2(Bucket=bucket_name)
        if 'Contents' in response:
            print('{:13} {:25}   {:10}   {}(Total scanned files={}) '.format('Action', 'Date', 'Size', 'Filename', len(response['Contents']) ))
            for item in response['Contents']:
                if(item['Key'].split('.')[-1] in delete_types):
                    print('{:13} {} : {:10} : {} '.format('deleting file', item['LastModified'], item['Size'], item['Key']))           
                    s3c.delete_object(Bucket=bucket_name, Key=item['Key'])

                if(item['Key'].split('/')[-1] in delete_files):
                    print('{:13} {} : {:10} : {} '.format('deleting file', item['LastModified'], item['Size'], item['Key']))           
                    s3c.delete_object(Bucket=bucket_name, Key=item['Key'])
    except ClientError as e:
        print("Check if the bucket {} exists!".format(bucket_name))
        print("Exception message : "+e)
        return

    
def main():
    
    ps_start = time()
    print('{} : Starting S3 Upload process'.format(dt.datetime.now()))

    config = configparser.ConfigParser()
    config.read_file(open('./config.cfg'))
    
    KEY                    = config.get('AWS','KEY')
    SECRET                 = config.get('AWS','SECRET')
 
    DB_NAME                = config.get("CLUSTER","DB_NAME")
    DB_USER                = config.get("CLUSTER","DB_USER")
    DB_PASSWORD            = config.get("CLUSTER","DB_PASSWORD")
    DB_PORT               = config.get("CLUSTER","DB_PORT")

    
    bucket_name = config.get("S3", "BUCKET")
    
    s3c = boto3.client('s3',
                           region_name="us-west-2",
                           aws_access_key_id=KEY,
                           aws_secret_access_key=SECRET
                         )

    s3r = boto3.resource('s3',
                           region_name="us-west-2",
                           aws_access_key_id=KEY,
                           aws_secret_access_key=SECRET
                         )
        
    location = {'LocationConstraint': 'us-west-2'}
    #bucket_name = 'calamaribucket-capstone'    
    
    # path to folder which needs to be zipped 
    folders = ['inputs', 'outputs']
    
    # sets S3 path
    input_data = "s3a://calamaribucket-capstone"
    out_data = "s3a://calamaribucket-capstone"

    # calling function to get all file paths in the directory 
    selected_files, ignored_files = local_get_all_files(folders)    
    
    """
    print('Selected files = ',len(selected_files))
    for num, fp in enumerate(selected_files, start=1):
        print('{}. {}'.format(num, fp))
    """

    """
    print('Ignored files = ',len(ignored_files))
    for num, fp in enumerate(ignored_files, start=1):
        print('{}. {}'.format(num,fp))
    """        
    # /home/workspace/

    s3_list_buckets(s3c)
    print('{} : Creating bucket {}'.format(dt.datetime.now(), bucket_name))
    s3_create_bucket(s3c, s3r, location, bucket_name)
    
    """
    # Uploads the given file using a managed uploader, which will split up large
    # files automatically and upload parts in parallel.    
    filename = 'ddl.txt'    
    s3c.upload_file(filename, bucket_name, 'hi/'+filename)    
    """

    print('{} : Uploading CSV files to bucket {}'.format(dt.datetime.now(), bucket_name))
    rFindStr='/home/workspace/'
    rStr = ''
    s3_upload_files(s3c, bucket_name, selected_files, rFindStr, rStr)

    #print('{} : Uploading Parquet files to bucket {}'.format(dt.datetime.now(), bucket_name))
    #filepath = 'outputs-parquet/i94-apr16.parquet'
    #s3_upload_parquet_files(s3r, bucket_name, 'i94-apr16.parquet', filepath)

    print('{} : Uploading gzip files to bucket {}'.format(dt.datetime.now(), bucket_name))
    filepath = 'outputs-gzip/immigration_dfs1.gzip'
    s3_upload_parquet_files(s3r, bucket_name, 'immigration_dfs1.gzip', filepath)
    
    # Delete crc & _SUCCESS files
    print('Deleting crc & _SUCCESS files')
    delete_files = ['_SUCCESS']
    delete_types = ['crc']    
    delete_filetype_in_s3_bucket(s3c, bucket_name, delete_files, delete_types)    
    # Deletes a bucket
    # s3_delete_bucket(s3r, bucket_name)
    
    ps_et = time() - ps_start
    print("=== {} Total Elapsed time is {} sec\n".format('S3 Upload process', round(ps_et,2) ))
    print('{} : Done!'.format(dt.datetime.now()))
    
    
    
if __name__ == "__main__":
    main()    