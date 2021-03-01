from airflow.models import DAG, Variable
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from tasks.slack_dag_failure_alert import slack_failed_task
import psycopg2
import pandas as pd
from airflow_home.variables import *
from sqlalchemy import create_engine


def kill_queries_longer_than_one_hour():
    """Kill Redshift Queries running longer than an hour on the hour for non vetted people.
       First pg_cancel, then pg_terminate"""

    excluded_user_list = Variable.get("redshift_long_running_queries_kill_exclusion_list",
                                      "pslack, cmorss").split(", ")

    engine = create_engine(SQL_ALCH_REDSHIFT_CONN_STRING)

    sql = """
    SELECT
    sr.pid
    FROM stv_recents sr
    LEFT JOIN stl_query sq
    ON sr.query = sq.query
    WHERE 1=1
    AND status = 'Running'
    --- exclude exempt super users
    AND trim(sr.user_name) NOT IN %(excluded_user_list)s
    ----kill queries running greater than an hour
    AND sr.duration >= 3600000000;
    """

    query_params = {'excluded_user_list': tuple(excluded_user_list)}
    df_query_results = pd.read_sql_query(sql, engine, params=query_params)
    if df_query_results.empty:
        pass
    else:
        df = df_query_results.copy()

        conn = psycopg2.connect(REDSHIFT_CONN_STRING)
        cursor = conn.cursor()

        pid_list = df['pid'].values.tolist()

        for pids in pid_list:
            sql = ("""
            SELECT pg_cancel_backend(%(schemaname)s);
                """
                   ) % {'pids': pids}

            cursor.execute(sql)
            conn.commit()

        conn.close()

        engine = create_engine(SQL_ALCH_REDSHIFT_CONN_STRING)

        sql = """
        SELECT
        sr.pid
        FROM stv_recents sr
        LEFT JOIN stl_query sq
        ON sr.query = sq.query
        WHERE 1=1
        AND sr.pid IN %(pid_list)s
        """

        query_params = {'pid_list': tuple(pid_list)}
        df_query_results = pd.read_sql_query(sql, engine, params=query_params)
        if df_query_results.empty:
            pass
        else:
            df = df_query_results.copy()

            conn = psycopg2.connect(REDSHIFT_CONN_STRING)
            cursor = conn.cursor()

            pid_list = df['pid'].values.tolist()

            for pids in pid_list:
                sql = ("""
                SELECT pg_terminate_backend(%(schemaname)s);
                """
                       ) % {'pids': pids}

                cursor.execute(sql)
                conn.commit()

            conn.close()


default_args = {
    'on_failure_callback': slack_failed_task
    , 'owner': 'pslack'
    , 'priority_weight': 5
}

dag = DAG(
    dag_id='redshift_kill_queries'
    , description='pg_cancel then pg_terminate queries longer than an hour.'
    , schedule_interval='@hourly'
    , start_date=datetime(2020, 5, 3)
    , catchup=False
    , default_args=default_args
    , tags=['utility']
)

kill_queries = PythonOperator(
    task_id='kill_queries_longer_than_one_hour'
    , python_callable=kill_queries_longer_than_one_hour
    , provide_context=False
    , dag=dag
)

kill_queries
