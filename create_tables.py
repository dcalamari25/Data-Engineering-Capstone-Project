import configparser
import psycopg2
from sql_queries import schema_queries, create_table_queries, drop_table_queries

config = configparser.ConfigParser()
config.read('config.cfg')


conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
#conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format( HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT ))
cur = conn.cursor()

def drop_tables(cur, conn):
    """
    Drop all the table present in the Redshift cluster
    :param cur: cursor connection object on Redshift
    :param conn: connection object on Redshift
    """
    print('create_tables line 58')
        
    for o in drop_table_queries:
        print(o['message'])
        print(o['query'])
        try:
            cur.execute(o['query'])
            conn.commit()
        except psycopg2.Error as e:
            print(e)
            conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
            cursor = conn.cursor()
            conn.close()                               

def create_tables(cur, conn):
    """
    Create table in the Redshift cluster
    :param cur: cursor connection object on Redshift
    :param conn: connection object on Redshift
    """
    print('create_tables line 127')

    for o in create_table_queries:
        print(o['message'])
        print(o['query'])
        try:
            cur.execute(o['query'])
            conn.commit()
        except psycopg2.Error as e:
            print(e)
            conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
            cursor = conn.cursor()
            conn.close()      
        
def create_schema(cur, conn):
    """
    Create table in the Redshift cluster
    :param cur: cursor connection object on Redshift
    :param conn: connection object on Redshift
    """
    print('create schema line 155')
    print(schema_queries[0]['message'])
    print(schema_queries[0]['query'])
    try:
        cur.execute('CREATE SCHEMA if not exists test;')
    except psycopg2.Error as e:
        print(e)
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
        cursor = conn.cursor()
        conn.close() 
        
def drop_schema(cur, conn):
    """
    Create table in the Redshift cluster
    :param cur: cursor connection object on Redshift
    :param conn: connection object on Redshift
    """
    print('drop schema line 180')
    print(schema_queries[1]['message'])  
    print(schema_queries[1]['query'])
    try:        
        cur.execute('DROP SCHEMA if exists test CASCADE;')
    except psycopg2.Error as e:
        print(e)
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
        cursor = conn.cursor()
        conn.close()                               

def main():
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

    for k, v in new_dict.items():
        globals()[k] = v



    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    #conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format( HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT ))
    cur = conn.cursor()

    drop_tables(cur, conn)
    drop_schema(cur, conn)
    
    create_schema(cur, conn)    
    create_tables(cur, conn)

    print('Done!')
    conn.close()

if __name__ == "__main__":
    main()