import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('config.cfg')

# Reading config & initializing variables
details_dict = {}
new_dict = {}
for section in config.sections():
    dic = dict(config.items(section))
    if len(dic) != 0 :
        #print( '{} - {} : {}'.format(len(dic), section, dic) )
        details_dict.update(dic)

        for k, v in details_dict.items():
            k = k.upper()
            #print('{} : {}'.format(k,v))
            new_dict[k] = v

            #print(new_dict)

            for k, v in new_dict.items():
                globals()[k] = v

                #print(' LOG_LOCAL_DATA = {}'.format(LOG_LOCAL_DATA))
    
    

# CREATE SCHEMA 
create_schema = {
    "query": "create schema if not exists;",
    "message": "CREATE schema"
}

# DROP SCHEMA 
drop_schema = {
    "query": "drop schema",
    "message": "DROP schema"    
}

# DROP STAGING TABLES
staging_immigration_drop = {
    "query": "DROP TABLE IF EXISTS staging_immigration;",
    "message": "DROP TABLE staging_immigration"
}

staging_PoE_drop = {
    "query": "DROP TABLE IF EXISTS staging_PoE ;",
    "message": "DROP TABLE staging_PoE "
}

staging_airport_codes_drop = {
    "query": "DROP TABLE IF EXISTS staging_airport_codes;",
    "message": "DROP TABLE staging_airport_codes"
}

staging_demog_drop = {
    "query": "DROP TABLE IF EXISTS staging_demog;",
    "message": "DROP TABLE staging_demog"
}

staging_race_drop = {
    "query": "DROP TABLE IF EXISTS staging_race;",
    "message": "DROP TABLE staging_race"
}

staging_worldtemp_drop = {
    "query": "DROP TABLE IF EXISTS staging_worldtemp;",
    "message": "DROP TABLE staging_worldtemp"
}


# DROP FACT & DIM TABLES
fact_immigration_drop = {
    "query": "DROP TABLE IF EXISTS fact_immigration;",
    "message": "DROP TABLE fact_immigration"
}

dim_PoE_drop = {
    "query": "DROP TABLE IF EXISTS dim_PoE ;",
    "message": "DROP TABLE dim_PoE "
}

dim_airport_codes_drop = {
    "query": "DROP TABLE IF EXISTS dim_airport_codes;",
    "message": "DROP TABLE dim_airport_codes"
}

dim_demog_drop = {
    "query": "DROP TABLE IF EXISTS dim_demog;",
    "message": "DROP TABLE dim_demog"
}

dim_race_drop = {
    "query": "DROP TABLE IF EXISTS dim_race;",
    "message": "DROP TABLE dim_race"
}

dim_worldtemp_drop = {
    "query": "DROP TABLE IF EXISTS dim_worldtemp;",
    "message": "DROP TABLE dim_worldtemp"
}

# CREATE Staging TABLES
staging_immigration_table_create = {
    "query": """
        CREATE TABLE IF NOT EXISTS "staging_immigration" (
          "CoC" VARCHAR,
          "CoR" VARCHAR,
          "PoE" VARCHAR,
          "arrival_state" VARCHAR,
          "age" VARCHAR,
          "biryear" VARCHAR,
          "gender" VARCHAR,
          "airline" VARCHAR,
          "admnum" VARCHAR,
          "fltno" VARCHAR,
          "visatype" VARCHAR,
          "arrival_mode" VARCHAR,
          "visit_purpose" VARCHAR,
          "arrival_dt" TIMESTAMP,
          "departure_dt" TIMESTAMP
        );
        """,
    "message": "CREATE TABLE staging_immigration"
}

staging_PoE_table_create = {
    "query": """
        CREATE TABLE IF NOT EXISTS "staging_PoE" (
        "code" VARCHAR,
        "citystate" VARCHAR,
        "city" VARCHAR,
        "state" VARCHAR
        );
        """,
    "message": "CREATE TABLE staging_PoE"
}

staging_airport_codes_table_create = {
    "query": """
        CREATE TABLE IF NOT EXISTS "staging_airport_codes" (
        "type" VARCHAR,
        "name" VARCHAR,
        "elevation_ft" DECIMAL(16,5),
        "iso_country" VARCHAR,
        "iso_region" VARCHAR,
        "municipality" VARCHAR,
        "iata_code" VARCHAR,
        "local_code" VARCHAR,
        "coordinates" VARCHAR,
        "longitude" VARCHAR,
        "latitude" VARCHAR,
        "state" VARCHAR
        );
        """,
    "message": "CREATE TABLE staging_airport_codes"
}

staging_demog_table_create = {
    "query": """
        CREATE TABLE IF NOT EXISTS "staging_demog" (
        "State" VARCHAR,
        "City" VARCHAR,
        "Median_Age" DECIMAL(16,5),
        "State_Code" VARCHAR,
        "Male_Population" INTEGER,
        "Female_Population" INTEGER,
        "Total_Population" INTEGER,
        "Number_of_Veterans" INTEGER,
        "Foreign_born" INTEGER
        );
        """,
    "message": "CREATE TABLE staging_demog"
}

staging_race_table_create = {
    "query": """
        CREATE TABLE IF NOT EXISTS "staging_race" (
        "State" VARCHAR,
        "City" VARCHAR,
        "American_Indian_and_Alaska_Native" INTEGER,
        "Asian" INTEGER,
        "Black_or_African_American" INTEGER,
        "Hispanic_or_Latino" INTEGER,
        "White" INTEGER
        );
        """,
    "message": "CREATE TABLE staging_race"
}

staging_worldtemp_table_create = {
    "query": """    
        CREATE TABLE IF NOT EXISTS "staging_worldtemp" (
        "dt" TIMESTAMP,
        "AverageTemperature" DECIMAL(16,5),
        "City" VARCHAR,
        "Country" VARCHAR
        );    
        """,
    "message": "CREATE TABLE staging_worldtemp"
}

# CREATE Fact & Dim TABLES
fact_immigration_table_create = {
    "query": """
        CREATE TABLE IF NOT EXISTS "fact_immigration" (
        "CoC" VARCHAR,
        "CoR" VARCHAR,
        "PoE" VARCHAR,
        "arrival_state" VARCHAR,
        "age" VARCHAR,
        "biryear" VARCHAR,
        "gender" VARCHAR,
        "airline" VARCHAR,
        "admnum" VARCHAR,
        "fltno" VARCHAR,
        "visatype" VARCHAR,
        "arrival_mode" VARCHAR,
        "visit_purpose" VARCHAR,
        "arrival_dt" TIMESTAMP,
        "departure_dt" TIMESTAMP,
        CONSTRAINT fact_immigration_pk PRIMARY KEY ("age", "arrival_dt", "CoR")
        );
        """,
    "message": "CREATE TABLE fact_immigration"
}

dim_airport_codes_table_create = {
    "query": """
        CREATE TABLE IF NOT EXISTS "dim_airport_codes" (
        "type" VARCHAR,
        "name" VARCHAR,
        "elevation_ft" DECIMAL(16,5),
        "iso_country" VARCHAR,
        "iso_region" VARCHAR,
        "municipality" VARCHAR,
        "iata_code" VARCHAR,
        "local_code" VARCHAR,
        "coordinates" VARCHAR,
        "longitude" VARCHAR,
        "latitude" VARCHAR,
        "state" VARCHAR,
        CONSTRAINT dim_airport_codes_pk PRIMARY KEY ("type", "name")
        );    
        """,
    "message": "CREATE TABLE dim_airport_codes"
}

dim_PoE_table_create = {
    "query": """
        CREATE TABLE IF NOT EXISTS "dim_PoE" (
        "code" VARCHAR,
        "citystate" VARCHAR,
        "city" VARCHAR,
        "state" VARCHAR,
        CONSTRAINT dim_PoE_pk PRIMARY KEY ("code")
        );
        """,
    "message": "CREATE TABLE dim_PoE"
}

dim_demog_table_create = {
    "query": """
        CREATE TABLE IF NOT EXISTS "dim_demog" (
        "State" VARCHAR,
        "City" VARCHAR,
        "Median_Age" DECIMAL(16,5),
        "State_Code" VARCHAR,
        "Male_Population" INTEGER,
        "Female_Population" INTEGER,
        "Total_Population" INTEGER,
        "Number_of_Veterans" INTEGER,
        "Foreign_born" INTEGER,
        CONSTRAINT dim_demog_pk PRIMARY KEY ("State", "City")
        );
        """,
    "message": "CREATE TABLE dim_demog"
}

dim_race_table_create = {
    "query": """
        CREATE TABLE IF NOT EXISTS "dim_race" (
        "State" VARCHAR,
        "City" VARCHAR,
        "American_Indian_and_Alaska_Native" INTEGER,
        "Asian" INTEGER,
        "Black_or_African_American" INTEGER,
        "Hispanic_or_Latino" INTEGER,
        "White" INTEGER,
        CONSTRAINT dim_race_pk PRIMARY KEY ("State", "City")
        );    
        """,
    "message": "CREATE TABLE dim_race"
}

dim_worldtemp_table_create = {
    "query": """
        CREATE TABLE IF NOT EXISTS "dim_worldtemp" (
        "dt" TIMESTAMP,
        "AverageTemperature" DECIMAL(16,5),
        "City" VARCHAR,
        "Country" VARCHAR,
        CONSTRAINT dim_worldtemp_pk PRIMARY KEY ("dt", "City", "Country")
        );        
        """,
    "message": "CREATE TABLE dim_worldtemp"
}

# STAGING TABLES
'''
staging_events_copy_query = ("""
    COPY staging_events FROM {}
    CREDENTIALS '{}'
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
    CSV 
    delimiter ',' 
    IGNOREHEADER 1
    COMPUPDATE OFF REGION 'us-west-2';
    """).format(config.get('S3', 'calamaribucket-capstone'))
    
    #format(CLEAN_LOG_DATA, KEYSECRET)
staging_events_copy = {
    "query": staging_events_copy_query,
    "message": "COPY staging_events"
}
'''

# FINAL INSERTs to FACT & DIMENSION tables
dim_airport_codes_insert = {
    "query": ("""
        insert into dim_airport_codes(type, name, elevation_ft, iso_country, iso_region, municipality, iata_code, local_code, coordinates, longitude, latitude, state)
        select distinct  type, name, elevation_ft, iso_country, iso_region, municipality, iata_code, local_code, coordinates, longitude, latitude, state 
        from staging_airport_codes ;
        """),
    "message": "INSERT INTO dim_airport_codes TABLE"
}

dim_PoE_insert = {
    "query": ("""
        insert into dim_PoE(code, citystate, city, state)
        select distinct  code, citystate, city, state 
        from staging_PoE ;  
        """),
    "message": "INSERT INTO dim_PoE TABLE"
}

dim_demog_insert = {
    "query": ("""
        insert into dim_demog(State, City, Median_Age, State_Code, Male_Population, Female_Population, Total_Population, Number_of_Veterans, Foreign_born)
        select distinct  State, City, Median_Age, State_Code, Male_Population, Female_Population, Total_Population, Number_of_Veterans, Foreign_born 
        from staging_demog ;  
        """),
    "message": "INSERT INTO dim_demog TABLE"
}

dim_race_insert = {
    "query": ("""
        insert into dim_race(State, City, American_Indian_and_Alaska_Native, Asian, Black_or_African_American, Hispanic_or_Latino, White)
        select distinct  State, City, American_Indian_and_Alaska_Native, Asian, Black_or_African_American, Hispanic_or_Latino, White
        from staging_race ; 
        """),
    "message": "INSERT INTO dim_race TABLE"
}

dim_worldtemp_insert = {
    "query": ("""
        insert into dim_worldtemp(dt, AverageTemperature, City, Country)
        select distinct  dt, AverageTemperature, City, Country
        from staging_worldtemp ;  
        """),
    "message": "INSERT INTO dim_worldtemp TABLE"
}

fact_immigration_insert = {
    "query": ("""
        insert into fact_immigration(CoC, CoR, PoE, arrival_state, age, biryear, gender, airline, admnum, fltno, visatype, arrival_mode, visit_purpose, arrival_dt, departure_dt)
        select distinct  CoC, CoR, PoE, arrival_state, age, biryear, gender, airline, admnum, fltno, visatype, arrival_mode, visit_purpose, arrival_dt, departure_dt
        from staging_immigration ;
        """),
    "message": "INSERT INTO fact_immigration TABLE"
}

# count rows 
'''
count_staging_events = {
    "query" : ("""
    SELECT COUNT(*) FROM staging_events ;
    """),
    "message" : "Rows in Staging Events"
}
'''

# QUERY LISTS
schema_queries = [create_schema, drop_schema]

create_table_queries = [staging_immigration_table_create, staging_airport_codes_table_create, staging_PoE_table_create, staging_demog_table_create, staging_race_table_create, staging_worldtemp_table_create, fact_immigration_table_create, dim_airport_codes_table_create, dim_PoE_table_create, dim_demog_table_create, dim_race_table_create, dim_worldtemp_table_create]

drop_table_queries = [fact_immigration_drop, dim_airport_codes_drop, dim_PoE_drop, dim_demog_drop, dim_race_drop, dim_worldtemp_drop, staging_immigration_drop, staging_airport_codes_drop, staging_PoE_drop, staging_demog_drop, staging_race_drop, staging_worldtemp_drop]

insert_table_queries = [dim_airport_codes_insert, dim_PoE_insert, dim_demog_insert, dim_race_insert, dim_worldtemp_insert, fact_immigration_insert]