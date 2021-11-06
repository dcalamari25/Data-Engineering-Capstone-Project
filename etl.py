import configparser
import psycopg2
import json
import sql_queries
import os

from importlib import reload
reload(sql_queries)
from sql_queries import schema_queries, insert_table_queries

import boto3

config = configparser.ConfigParser()
config.read('config.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['KEY']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['SECRET']


def make_copy_statement(table, data, cur):
    IAM_ROLE = 'arn:aws:iam::957808882659:role/myRedshiftRole25'
    copy_stmt = ("""
        COPY {} FROM {}
        IAM_ROLE '{}'
        TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
        CSV 
        delimiter ',' 
        IGNOREHEADER 1
        COMPUPDATE OFF REGION 'us-west-2';
        """).format(table, data, IAM_ROLE)
    return copy_stmt

def load_staging_immigration_table(cur, conn, fname):
    """
    Load the data from the outputs to the staging table in Redshift
  
    Parameters: 
    arg1 : cursor connection object on redshift
    arg2 : connection object on the redshift
  
    Returns: 
    None
    """    

    """
    print('\r{:5}* {}'.format('',schema_queries[2]['message']))    
    cur.execute(schema_queries[2]['query'])
    
    for o in copy_table_queries:
        print('\r{:5}* {}'.format('',o['message']))    
        try:
            cur.execute(o['query'])
            conn.commit()
        except psycopg2.Error as e:
            print(e)
            conn.close()
    """            
    print('load staging immigration table line 127')

    bucket = s3.Bucket(bucket_name)
    for subdir, dirs, files in os.walk(path):
        for file in files:
            full_path = os.path.join(subdir, file)
            with open(full_path, 'rb') as data:
                print('Fullpath : ',full_path)
                print(full_path[len(path)+1:])
                print(folder+'/'+full_path[len(path)+1:])
 
    for table, data in table_data.items():        
        if(data == 'ignore'):
            continue            
        
        print('Loading {} with data from {}'.format(table, data))
        copy_stmt = make_copy_statement(table, data, cur)
        print('line 165')
        try:
            print('line 167')
            cur.execute(copy_stmt)
            conn.commit()
        except psycopg2.Error as e:
            print('line 171')
            cur.execute("ROLLBACK")
            conn.commit() 
            cur.execute("SELECT * FROM STL_LOAD_ERRORS;")
            results = cur.fetchall()
            for row in results:
                print('inside for 176')
                print("   ", type(row), row)                            
                if(len(row) == 1):
                    print('{:8} {:25} - {}'.format(' ', table, row[0]))
            print(e)
            conn.close()   
            
def load_staging_tables(cur, conn, fname):
    """
    Load the data from the S3 bucket to the staging table in Redshift
  
    Parameters: 
    arg1 : cursor connection object on redshift
    arg2 : connection object on the redshift
  
    Returns: 
    None
    """    

    """
    print('\r{:5}* {}'.format('',schema_queries[2]['message']))    
    cur.execute(schema_queries[2]['query'])
    
    for o in copy_table_queries:
        print('\r{:5}* {}'.format('',o['message']))    
        try:
            cur.execute(o['query'])
            conn.commit()
        except psycopg2.Error as e:
            print(e)
            conn.close()
    """            
    print('load staging tables line 127')
    table_data = json.load(open(fname))
    for table, data in table_data.items():        
        if(data == 'ignore'):
            continue            
        
        print('Loading {} with data from {}'.format(table, data))
        copy_stmt = make_copy_statement(table, data, cur)
        print('line 148')
        try:
            print('line 150')
            cur.execute(copy_stmt)
            conn.commit()
        except psycopg2.Error as e:
            print('line 152')
            cur.execute("ROLLBACK")
            conn.commit() 
            cur.execute("SELECT * FROM STL_LOAD_ERRORS;")
            results = cur.fetchall()
            for row in results:
                print('inside for 160')
                print("   ", type(row), row)                            
                if(len(row) == 1):
                    print('{:8} {:25} - {}'.format(' ', table, row[0]))
            print(e)
            conn.close()        
    
def insert_tables(cur, conn):
    """
    Insert data from the staging table to the dimension and fact table
    :param cur: cursor connexion object on redshift
    :param conn: connection object on the redshift
    """
    print('insert tables line 152')
   
    for o in insert_table_queries:
        print('inside for insert line 273')
        print(o['message'])
        print(o['query'])
        print('query')
        try:
            cur.execute(o['query'])
            print('line 278')
            conn.commit()
        except psycopg2.Error as e:
            print(e)
            conn.close()

# Counts are the quality checks to ensure count checks on fact and dimension tables.            
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
                        
def main():
    config = configparser.ConfigParser()
    #config.read('config.cfg')

    # Upload cleaned files to s3    
    config = configparser.ConfigParser()
    config.read('config.cfg')

    os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['KEY']
    os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['SECRET']
    
    #print('LOG_LOCAL_DATA = ',LOG_LOCAL_DATA)
    #print('SONG_LOCAL_DATA = ',SONG_LOCAL_DATA)
    print('Bucket = ','BUCKET')
    print( *config['CLUSTER'].values() )

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()        
        
    print('1. Loading Staging tables')
    fname = './data/staging-table-data-Copy1.txt'
    load_staging_tables(cur, conn,  fname)
    
    print('2. Insert into Fact & Dimension tables')
    insert_tables(cur, conn)

    print('3. Count Rows')
    count_rows_staging(cur, conn, fname)    
    
    conn.close()
    print('Done!')
    

if __name__ == "__main__":
    main()