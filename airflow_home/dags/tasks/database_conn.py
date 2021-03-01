import airflow_home.credential_vars
import psycopg2

connection_options = ('redshift', 'zeus')


class NotAValidDatabase(Exception):
    pass


def db_conn(source):
    global cursor
    global conn

    if source == 'redshift':

        conn = psycopg2.connect(airflow_home.credential_vars.redshift_conn_string)
        cursor = conn.cursor()

    elif source == 'zeus':

        conn = psycopg2.connect(airflow_home.credential_vars.zeus_conn_string)
        cursor = conn.cursor()

    else:

        raise NotAValidDatabase(("Invalid Database Source. Please choose one"
                                 "of the following: %s")
                                % ', '.join(connection_options))

    return conn, cursor
