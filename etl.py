import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries, validation_queries, staging_validation_queries
import json

def load_staging_tables(cur, conn):
    """
    Loads staging data from S3 into staging tables via `copy_table_queries` list.
    """
    for query in copy_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except Exception as exc:
            print('Unexpected error running copy query: {} {}'.format(query, exc))
            cur.execute('rollback')


def insert_tables(cur, conn):
    """
    Selects data from staging tables and imports into new data model schema via `insert_table_queries` list.
    """
    for query in insert_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except Exception as exc:
            print('Unexpected error running insert query: {} {}'.format(query, exc))
            cur.execute('rollback')
   
def validate_tables(cur, conn, queries):
    """
    Prints results from a collection of queries
    """
    for query in queries:
        try:
            cur.execute(query)
            result = cur.fetchone()
            print('{} - {}'.format(query, result[0]))
            conn.commit()
        except Exception as exc:
            print('Unexpected error running validation query: {} {}'.format(query, exc))
            cur.execute('rollback')


def main():
    """
    Main program entry point to connect to Redshift cluster and load, insert and validate data in tables
    """
    try:
        config = configparser.ConfigParser()
        config.read('config.cfg')

        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
        cur = conn.cursor()

        print ('######## LOADING STAGING DATA ###########')
        load_staging_tables(cur, conn)

        print ('######## STAGING DATA VALIDATAION ###########')
        validate_tables(cur, conn, staging_validation_queries)

        print ('######## LOADING DATA INTO STAR SCHEMA ###########')
        insert_tables(cur, conn)

        print ('######## TRANSFORMED DATA VALIDATAION ###########')
        validate_tables(cur, conn, validation_queries)

        conn.close()
    except Exception as exc:
        print('Unexpected error running program: {}'.format(exc))

if __name__ == "__main__":
    main()