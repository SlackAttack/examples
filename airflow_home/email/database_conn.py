from credentials_vars import redshift_conn_string, bs4_conn_string, bs3_conn_string, cg_conn_string
import psycopg2
import mysql.connector

connection_options = ('redshift', 'cg', 'bs4', 'bs3')

class NotAValidDatabase(Exception):
    pass

def db_conn(source):

    global cursor
    global conn

    if source == 'redshift':

        conn=psycopg2.connect(redshift_conn_string)
        cursor=conn.cursor()

    elif source == 'bs4':

        conn=psycopg2.connect(bs4_conn_string)
        cursor=conn.cursor()

    elif source == 'bs3':

        conn=mysql.connector.connect(**bs3_conn_string)
        cursor=conn.cursor()

    elif source == 'cg':

        conn=mysql.connector.connect(**cg_conn_string)
        cursor=conn.cursor()

    else:

        raise NotAValidDatabase("\nInvalid Database Source\nPlease choose one of the following:\n%s"%', '.join(connection_options))

    return conn, cursor
