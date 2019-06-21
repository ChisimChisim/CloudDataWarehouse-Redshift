import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

'''
Load log&song data to staging tables from S3
'''
def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()

'''
Load data in the staging tables to final tables
'''
def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

'''
Connect Redshift cluster and load data from S3
'''
def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # connect to Redshift cluster
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    # load log and song data to staging tables from S3
    load_staging_tables(cur, conn)
    # load data to final tables from staging tables
    insert_tables(cur, conn)

    # close connection
    conn.close()


if __name__ == "__main__":
    main()