from airflow.models import DAG, Variable
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from tasks.slack_dag_failure_alert import slack_failed_task
import psycopg2
import pandas as pd
from airflow_home.variables import *
from sqlalchemy import create_engine, MetaData


def grant_redshift_access():
    """Grant Read/Read Write to the respective non admin groups that should default get access to all schemas
        with exceptions. Note: I am not adding those outside of  'ro_group', 'read_write_group'
        as an argument as I(pslack) would like approval before expanding the groups"""

    excluded_schema_list = Variable.get("grant_redshift_access_schema_exclusion_list",
                                        "pg_catalog, information_schema").split(", ")

    engine = create_engine(SQL_ALCH_REDSHIFT_CONN_STRING)

    sql = """
    SELECT
    DISTINCT schemaname
    FROM pg_tables
    WHERE 1=1
    AND schemaname not in %(excluded_schema_list)s
    ORDER BY 1;
    """

    query_params = {'excluded_schema_list': tuple(excluded_schema_list)}
    df_query_results = pd.read_sql_query(sql, engine, params=query_params)
    if df_query_results.empty:
        pass
    else:
        df = df_query_results.copy()

        conn = psycopg2.connect(REDSHIFT_CONN_STRING)
        cursor = conn.cursor()

        read_only_user_groups = ['ro_group']
        for user_group in read_only_user_groups:
            for schemaname in df['schemaname'].values.tolist():
                sql = ("""
                GRANT USAGE ON SCHEMA %(schemaname)s TO GROUP %(user_group)s;
                GRANT SELECT ON ALL TABLES IN SCHEMA %(schemaname)s TO GROUP %(user_group)s;
                ALTER DEFAULT PRIVILEGES FOR USER pslack, airflow IN SCHEMA %(schemaname)s
                GRANT SELECT ON TABLES TO GROUP %(user_group)s;
                """
                       ) % {'schemaname': schemaname, 'user_group': user_group}

                cursor.execute(sql)
                conn.commit()

        read_write_user_groups = ['read_write_group']
        for user_group in read_write_user_groups:
            for schemaname in df['schemaname'].values.tolist():
                sql = ("""
                GRANT USAGE,CREATE ON SCHEMA %(schemaname)s TO GROUP %(user_group)s;
                GRANT SELECT, INSERT, UPDATE, DELETE, REFERENCES ON ALL TABLES IN SCHEMA %(schemaname)s TO GROUP %(user_group)s;
                alter default privileges for user pslack, airflow IN SCHEMA %(schemaname)s grant SELECT, INSERT, UPDATE, DELETE, REFERENCES ON
                TABLES TO GROUP %(user_group)s;
                """
                       ) % {'schemaname': schemaname, 'user_group': user_group}

                cursor.execute(sql)
                conn.commit()

        conn.close()


default_args = {
    'on_failure_callback': slack_failed_task
    , 'owner': 'pslack'
    , 'priority_weight': 1
    , 'retries': 1
}

dag = DAG(
    dag_id='redshift_utility'
    , description='Takes care of various dba/admin/permissions related Redshift tasks'
    , schedule_interval='25 3 * * *'
    , start_date=datetime(2020, 4, 28)
    , catchup=False
    , default_args=default_args
    , tags=['utility']
)

grant_redshift_access_to_groups = PythonOperator(
    task_id='grant_redshift_access_to_groups'
    , python_callable=grant_redshift_access
    , provide_context=False
    , dag=dag
)

grant_redshift_access_to_groups
