import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

'''
Delete tables if it exists
'''
def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

'''
Create tables
'''
def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

'''
Connect Redshift cluster and create tables
'''
def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    # connect to Redshift cluster
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    # create tables in Redshift cluster
    drop_tables(cur, conn)
    create_tables(cur, conn)

    # close connection 
    conn.close()


if __name__ == "__main__":
    main()