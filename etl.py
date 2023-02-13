import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """ Copys S3 data into staging tables on redshift.
    INPUT:
    cur : cursor object from psycopg2 connection to sparkifydb
    filepath : String containing filepath to file for processing
    OUTPUT:
    Copies data from S3 bucket into staging tables on redshift using queries
    from copy_table_queries in sql_queries.py
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """ Inserts data from Staging tables into analytic tables
    INPUT:
    cur : cursor object from psycopg2 connection to sparkifydb
    filepath : String containing filepath to file for processing
    OUTPUT:
    Copies data from staging tables into analytic tables on redshift using queries
    from insert_table_queries in sql_queries.py
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """Establishes connection to redshift database using psycopg2 then calls 
    functions to load staging tables from S3, then copy staged data into 
    analytic tables.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host= {***} dbname=dev user={***} password={***} port=5439".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()