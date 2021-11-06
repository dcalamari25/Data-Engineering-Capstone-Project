# Project Title
### Data Engineering Capstone Project

#### Project Summary

The objective of this project was to utlize the skills learned throughout the course.  This is an open ended project to show what we have learned.  I've choosen to use the datasets projeced by Udacity to complete the project.  The main data set includes data on immigraiton to the United States and supplementary datasets included data on airport codes, U.S. city demographics, and temperature data. The use case for this analytics database is to find immigration patterns to the U.S.  
All data for the project was loaded into S3 after cleaning the datasets.  The project was completed within the Udacity workspace.

The files included in the workspace includes:

- config.cfg - the contains the configuration that allows the other modules the access AWS.
- utility.py - this module contains the functions for cleaning the data.
- sql_queries.py - this module drops, creates, and inserts the staging tables and fact and dimensional tables.
- create_cluster.py - this module creates the connection to AWS redshift and S3.
- create_tables.py - this module processes the sql_queries for staging, creating and inserting the tables.
- etl.py - this module reads the S3 data, processes the data using Spark, and writes processed data as a set of dimensional and fact tables back to S3.
- delete_cluster.py - deletes the cluster to avoid unnecessary charges.
- Capstone Project EDA.ipynb - Jupyter notebook used to gather, assess, and clean the data.
- Run.ipynb - Jupyter notebook used to run py files.
- Capstone Project Submision.ipynb - Jupyter notebook used to validate and show process of pulling data from S3 works and analysis can be done on data warehouse.

The project follows the follow steps:
* Step 1: Scope the Project and Gather Data
* Step 2: Explore and Assess the Data
* Step 3: Define the Data Model
* Step 4: Run ETL to Model the Data
* Step 5: Complete Project Write Up

### Step 1: Scope the Project and Gather Data

#### Scope 
The scope for the project is to integrate i94 immigration data, airport code data, world temperature data and US demographic data to set up a data warehouse with fact and dimension tables to be utilized for data analysis.

Data Sets (supplied by Udacity):

- i94 immigration data - CSV format, this data sample which is from the US National Tourism and Trade Office
- i94 port data (a subset of the immigration data manually created into JSON. It is from the I94_SAS_Labels_Description.SAS file.) 
- airport code data - CSV format, this data is a simple table of airport codes and corresponding cities.
- US Demographics data - CSV format, this dataset contains population details of US cities and census-designated places including gender and race information.  This data came from OpenSoft.
- world temperature data - CSV format, this dataset conatins temperature data of various cities.

Tools used:
 
- Python
- Juypter Notebooks
- AWS S3
- AWS Redshift

#### Describe and Gather Data 

- I94 Immigration Sample Data 
    - immigration_data_sample.csv 
        - csv  
            -This data comes from the US National Tourism and Trade Office. A data dictionary is included in the workspace. There's a sample file so you can take a look at the data in csv format before reading it all in. The sample dataset is in CSV format.
- I94 Port data 
    - i94port.json
        - json
            - Shows US Port of Entry city names and their corresponding codes. Source : I94_SAS_Labels_Descriptions.SAS
- World Temperature Data
    - world_temperature.csv
        - csv
            - This dataset contains temperature data of various cities from 1700's - 2013. This dataset came from Kaggle. 
- U.S. City Demographic Data
    - us-cities-demographics.csv
        - csv
            - This dataset contains population details of all US Cities and census-designated places includes gender & race informatoin. This data came from OpenSoft.
- Airport Code Table
    - airport-codes_csv.csv
        - csv
            - This is a simple table of airport codes and corresponding cities. 
            
### Step 2: Explore and Assess the Data
#### Explore the Data 

1. Use pandas for exploratory data analysis to get an overview of these datasets and clean the datasets.
2. Split the datasets to dimensional tables to change column names for better understanding.
3. Utilize PySpark on one of the SAS data sets to test ETL data pipeline logic.

#### Cleaning Steps

#### Immigration data cleaning steps

***Quality***
- Delete incompleted & invalid rows
    - matflag is null
    - visatype GMT
    - i94mode other than 1, 2, 3 can be removed.
    - incomplete: gender in null
    - removed rows having invalid CoC & CoR\
    - remove Non-US Port of entry data
    - update genter X to O
 
***Tideness***

- Keep only these columns
    i94cit, i94res, i94port, i94addr, arrdate, i94mode, depdate, i94addr, i94bir, i94visa, biryear, gender, airline, admnum, fltno, visatype
- Convert float to string(i94cit, i94res, arrdate, i94mode, depdate, i94bir, i94visa, biryear, admnum
- Convert codes to descriptive column names for below columns
    - i94mode - How they arrived - arrival_mode
    - i94visa - Purpose of visit - visit_purpose
- Convert date formats
    - SAS encoded date (arrdate & depdate)
- Rename columns; i94bir=age, i94cit=CoC, i94res=CoR, i94port=PoE, i94addr=arrival_state, arrdate=arrival_dt, depdate=departure_dt
- Drop final set columns i94mode, i94visa, arrdate, depdate, dtadfile, dtaddto

#### Airport Code data cleaning steps

#### Quality : airport_codes_df

- Keeping only US International airports serving passenger flights with immigration.
    - Keep only US Airports
    - Eliminate rows with NULL IATA codes
    - Keep only airport types small_airport, medium_airport, and large_airport
    - Remove rows having 'Duplicate' in airport name
    
#### Tidiness

- Structure changes to the airport dateframe
    - Drop unused columns continent, gps_code, ident
    - Split geo-coordinates into seperate columns
    - Extract region code from iso_region
    
#### US Demographics data cleaning steps

***Quality***
- demographics_df : Updating city names as some don't match up with df_uszips
- demographics_df : Deleting rows which has Male & Female population as 0    
     
***Tideness***

- demographics_df : Below structure changes will be carried out in demographic dataframe
     a) Convert floats to ints for columns 'Male Population', 'Female Population', 'Number of Veterans', 'Foreign-born', 'Total Population', 'Count'
     b) Unmelting the race rows values to separate columns   
     
#### World Temperature data cleaning steps

***Quality***

- No quality issues found 
     
***Tideness***

- Convert dt to datetime from object
- drop columns AverageTemperatureUncertainty, Latitude, Longitude
- remove missing temperatures
- elimiated any duplicates
- keep only US temperatures


### Step 3: Define the Data Model
#### 3.1 Conceptual Data Model

After gathering, reviewing, and cleaning all the datasets, it seems the i94 data will be the central data point.

1. There will be staging tables needed to preprocess the data before loading it into the data warehouse tables.  

    - staging_immigration_table
    
        - "CoC" VARCHAR,
        - "CoR" VARCHAR,
        - "PoE" VARCHAR,
        - "age" VARCHAR,
        - "arrival_state" VARCHAR,
        - "biryear" VARCHAR,
        - "gender" VARCHAR,
        - "airline" VARCHAR,
        - "admnum" VARCHAR,
        - "fltno" VARCHAR,
        - "visatype" VARCHAR,
        - "arrival_mode" VARCHAR,
        - "visit_purpose" VARCHAR,
        - "arrival_dt" TIMESTAMP,
        - "departure_dt" TIMESTAMP

    - staging_PoE_table
        - "code" VARCHAR,
        - "citystate" VARCHAR,
        - "city" VARCHAR,
        - "state" VARCHAR

    - staging_airport_codes
        - "type" VARCHAR,
        - "name" VARCHAR,
        - "elevation_ft" DECIMAL(16,5),
        - "iso_country" VARCHAR,
        - "iso_region" VARCHAR,
        - "municipality" VARCHAR,
        - "iata_code" VARCHAR,
        - "local_code" VARCHAR,
        - "coordinates" VARCHAR,
        - "longitude" VARCHAR,
        - "latitude" VARCHAR,
        - "state" VARCHAR

    - staging_demog_table(
        - "State" VARCHAR,
        - "City" VARCHAR,
        - "Median_Age" DECIMAL(16,5),
        - "State_Code" VARCHAR,
        - "Male_Population" INTEGER,
        - "Female_Population" INTEGER,
        - "Total_Population" INTEGER,
        - "Number_of_Veterans" INTEGER,
        - "Foreign_born" INTEGER

    - staging_race_table
        - "State" VARCHAR,
        - "City" VARCHAR,
        - "American_Indian_and_Alaska_Native" INTEGER,
        - "Asian" INTEGER,
        - "Black_or_African_American" INTEGER,
        - "Hispanic_or_Latino" INTEGER,
        - "White" INTEGER

    - staging_worldtemp_table
        - "dt" TIMESTAMP,
        - "AverageTemperature" DECIMAL(16,5),
        - "City" VARCHAR,
        - "Country" VARCHAR

2. The data warehouse tables will be where the main fact and dimension tables will be created and loaded.

    - fact_immigration_table
        - "CoC" VARCHAR,
        - "CoR" VARCHAR,
        - "PoE" VARCHAR,
        - "age" VARCHAR,
        - "arrival_state" VARCHAR,
        - "biryear" VARCHAR,
        - "gender" VARCHAR,
        - "airline" VARCHAR,
        - "admnum" VARCHAR,
        - "fltno" VARCHAR,
        - "visatype" VARCHAR,
        - "arrival_mode" VARCHAR,
        - "visit_purpose" VARCHAR,
        - "arrival_dt" TIMESTAMP,
        - "departure_dt" TIMESTAMP,
        - CONSTRAINT fact_immigration_pk PRIMARY KEY ("age", "arrival_dt", "CoR")

    - dim_airport_codes_table
        - "type" VARCHAR,
        - "name" VARCHAR,
        - "elevation_ft" DECIMAL(16,5),
        - "iso_country" VARCHAR,
        - "iso_region" VARCHAR,
        - "municipality" VARCHAR,
        - "iata_code" VARCHAR,
        - "local_code" VARCHAR,
        - "coordinates" VARCHAR,
        - "longitude" VARCHAR,
        - "latitude" VARCHAR,
        - "state" VARCHAR,
        - CONSTRAINT dim_airport_codes_pk PRIMARY KEY ("type", "name")

    - dim_PoE_table
        - "code" VARCHAR,
        - "citystate" VARCHAR,
        - "city" VARCHAR,
        - "state" VARCHAR,
        - CONSTRAINT dim_PoE_pk PRIMARY KEY ("code")

    - dim_demog_table
        - "State" VARCHAR,
        - "City" VARCHAR,
        - "Median_Age" DECIMAL(16,5),
        - "State_Code" VARCHAR,
        - "Male_Population" INTEGER,
        - "Female_Population" INTEGER,
        - "Total_Population" INTEGER,
        - "Number_of_Veterans" INTEGER,
        - "Foreign_born" INTEGER,
        - CONSTRAINT dim_demog_pk PRIMARY KEY ("State", "City")

    - dim_race_table
        - "State" VARCHAR,
        - "City" VARCHAR,
        - "American_Indian_and_Alaska_Native" INTEGER,
        - "Asian" INTEGER,
        - "Black_or_African_American" INTEGER,
        - "Hispanic_or_Latino" INTEGER,
        - "White" INTEGER,
        - CONSTRAINT dim_race_pk PRIMARY KEY ("State", "City")
    
    - dim_worldtemp_table
        - "dt" TIMESTAMP,
        - "AverageTemperature" DECIMAL(16,5),
        - "City" VARCHAR,
        - "Country" VARCHAR,
        - CONSTRAINT dim_worldtemp_pk PRIMARY KEY ("dt", "City", "Country")
        
#### 3.2 Mapping Out Data Pipelines

There much consideration put into the process of validating missing data, ensuring quality data, and determining where dirty data was so it could be discarded.  

All data quality issues and tidiness issues are identified clearly in Step 2: Explore and Assess the Data.  

Steps followed were:

- import libraries
- load configurations
- create spark session
- load datasets
- explore and assess datasets
- clean datasets
- save cleaned datasets to S3
- create dimension and fact tables
- load data into tables

### Step 4: Run Pipelines to Model the Data 
#### 4.1 Create the data model

The data model is built in the etl.py file and the data dictionaries are in the data_dictionary folder.  The data model and data dictionaries will be displayed in the readme.md file.

Below are the Fact and Dimension Tables used for this project:

***Fact Table***
- *fact_immigration*: Contains details on travelers entering the US.

***Dimension Tables***
- *dim_airport_codes*: This table contains information on US airports.
- *dim_PoE*: This is a reference table containing information on US Port of Entry codes, city and state names.
- *dim_demog*: This table contains information on population details in city and states.
- *dim_race*: This table contains information on various races in city and states.
- *dim_worldtemp*: This table contains information on temperatures globally with city and country.

#### 4.2 Data Quality Checks

In the Capstone Project Submission.ipynb file the date quality checkes were performed 
 
Run Quality Checks code is below.

def count_rows(cur, conn):
    """
    Get the number of rows stored into each table
  
    Parameters: 
    arg1 : cursor connection object on redshift
    arg2 : connection object on the redshift
  
    Returns: 
    None
    """    
    print('line 328')
    print('\r{:5}* {}'.format('',schema_queries[2]['message']))    
    cur.execute(schema_queries[2]['query'])
    
    for o in count_rows_queries:
        print('\r{:5}* {}'.format('',o['message']))    
        cur.execute(o['query'])
        print('query')
        #results = cur.fetchone()
        results = cur.fetchall()

        for row in results:
            if(len(row) == 1):
                print('{:8} {}'.format(' ', row[0]))
                print('line 342')
            else:
                print('{:8} {} - {}'.format(' ', row[0], row[1]))
                print('line 345')
            #print("   ", type(row), row)

def count_rows_staging(cur, conn, fname):
    """
    Get the number of rows stored into each table
  
    Parameters: 
    arg1 : cursor connection object on redshift
    arg2 : connection object on the redshift
  
    Returns: 
    None
    """    
    print('line 359')
    
    table_data = json.load(open(fname))
    for table, data in table_data.items():
        print('line 365')
        sql_stmt = 'SELECT COUNT(*) FROM {}'.format(table)
        try:
            cur.execute(sql_stmt)
            results = cur.fetchall()
            print('line 371')
            for row in results:
                #print("   ", type(row), row)                            
                if(len(row) == 1):
                    print('{:8} {:25} - {}'.format(' ', table, row[0]))
                    print('line376')
        except psycopg2.Error as e:
            print(e)
            conn.close()        
            
#### 4.3 Data dictionary 

There are data quality checks completed during the project.  During the etl.py the counts validate data was inserted into the tables. 

![fact_immigration data dictionary](./data_dictionary/Data dictionary - Project Capstone fact_immigration.pdf)

![dim_airport_codes data dictionary](./data_dictionary/Data dictionary - Project Capstone dim_airport_codes.pdf)

![dim_demog data dictionary](./data_dictionary/Data dictionary - Project Capstone dim_demog.pdf)

![dim_PoE data dictionary](./data_dictionary/Data dictionary - Project Capstone dim_PoE.pdf)

![dim_race data dictionary](./data_dictionary/Data dictionary - Project Capstone dim_race.pdf)

![dim_worldtemp data dictionary](./data_dictionary/Data dictionary - Project Capstone dim_worldtemp.pdf)


#### 4.4 Analytics
![Top reason US visited chart](./analytics_images/Q1.png)

![States most visited](./analytics_images/Q2.png)

![US State with highest port of entry](./analytics_images/Q3.png)

*Florida* is the most visited US state.  It is often referred to as the Sunshine State, and is a major tourist attraction due to its various beaches and parks. The largest attraction that brings tourist from around the world if Walt Disney World theme park.  There are many other amusement parks around the state including Universal Studios Busch Gardens and Sea World.  There are beautiful beaches lining most of the state. Other notable sites in Florida include the Castillo de San Marcos, the Everglades, South Beach, and the famous Overseas Highway, which connects the Florida Keys to the mainland.

*New York* is the second most visited US state.  It has a large varity of attractions.  The main attractions are in New York city which is sometimes referred to as the city that never sleeps.  The city draws visitors from around the world for it famous luxury hotels, highly ranked resturants, theater, ballet, and festivals.  There are parts of the state that are very much city hussle and other parts that are beautiful country.

*California* is the third most visited US state. The state's vast area is home to various vibrant cities, amusement parks, beaches and natural wonders that are popular among travelers from America and the rest of the world. Both Los Angeles and San Francisco are considered Gateway Cities, and are home to some of the most popular and recognizable tourist spots in the country. These include the Golden Gate Bridge, Disneyland, and Hollywood. Additionally, some of the national parks located in California are also significant tourist destinations. For example, Yosemite National Park in Northern California is well know for its tall mountains, valleys, and beautiful waterfalls. The weather is perfect and varies throughout the state from desert climate to snowy mountatins to balmy beaches.

#### Step 5: Complete Project Write Up
Tools & Technology used:
- Python: Python was used to perform the data analysis as it supports a wide variety of libraries to help perform tasks faster.
- Pandas: Pandas is very useful in gathering and asseing initial datasets and analysis.
- Spark: Pyspark is used to process big data.
- AWS Redshift: RedShift is a warehousing database that allows performing query analysis.
- AWS S3: S3 is used to stored processed outputs.

Python was used because supports a wide variety of libraries that helps perform inital data analysis faster.

Pandas was used because it is a useful way to gather and assess data and understand what is included in the datasets.

Spark is chosen for this project as it is known for processing large amount of data fast (with in-memory compute), scale easily with additional worker nodes, with ability to digest different data formats (e.g. SAS, Parquet, CSV), and integrate nicely with cloud storage like S3 and warehouse like Redshift.

The data update cycle is typically chosen on two criteria. One is the reporting cycle, the other is the availabilty of new data to be fed into the system. For example, if new batch of average temperature can be made available at monthly interval, we might settle for monthly data refreshing cycle.

### Alternative approach to solve problem under different scenarios.
### The data was increased by 100X
To process 28 million rows by ```utility.py``` takes around 2.5hrs in the Udacity workspace. 100x more data probably would take 10x - 100x more time, thats lot of time.  And the workspace would probably timeout.  So this would not be a sustainable approach. A better approach would be, input data should be stored in AWS S3 and data processing should be done in AWS EMR. If the input data is zipped, it would save on S3 storage and Pyspark can read zip files. Redshift can handle ~2.8 Billion rows easily. Being a warehousing database of choice in AWS, it should handle the increased data.

### The data populates a dashboard that must be updated on a daily basis by 7am every day
Scheduling can be setup easily by using Airflow to schedle the runs daily by 7:00 am via an ETL and dags.  Data quality checks would be crucial to ensure high data integrity and trust in the data.

### The database needs to be assessed by 100+ people
AWS RedShift can support up to 500 connections and users can be given access and different levels on what access is allowed.  AWS S3 can also handle the permissions and access to the data.  But you would have maintain proper access because the cost could get expensive if this is not carefully watched and updated.