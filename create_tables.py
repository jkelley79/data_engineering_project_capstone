import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Drops tables listed in `drop_table_queries` collection.
    """
    for query in drop_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except Exception as exc:
            print('Unexpected error running drop query: {} {}'.format(query, exc))
            cur.execute('rollback')

def create_tables(cur, conn):
    """
    Creates tables listed in `create_table_queries` collection.
    """
    for query in create_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except Exception as exc:
            print('Unexpected error running create query: {} {}'.format(query, exc))
            cur.execute('rollback')


def main():
    """
    Main program entry point to connect to Redshift cluster and drop/create data tables
    """
    try:
        config = configparser.ConfigParser()
        config.read('config.cfg')

        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
        cur = conn.cursor()

        drop_tables(cur, conn)
        create_tables(cur, conn)

        conn.close()
    except Exception as exc:
        print('Unexpected error running program: {}'.format(exc))


if __name__ == "__main__":
    main()