import io
import csv
import os, sys
import boto3
import pandas as pd
from airflow import DAG
from botocore.client import Config
from datetime import datetime, timedelta
from dag_objects import GoogleSheets, RedshiftCopy, s3
from airflow.operators.load_csv_to_s3 import LoadCsvToS3
from tasks.slack_dag_failure_alert import slack_failed_task
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.bash_operator import BashOperator

AIRFLOW_HOME = os.environ['AIRFLOW_HOME']

sys.path.append(AIRFLOW_HOME)

from modules.facebook_reporting import FacebookReporting
from modules.google_ads_reporting import GoogleAdsReporting
from credentials_vars import (facebook_client_token, facebook_app_id,
    facebook_app_secret, facebook_ad_account_id, s3_access_secret_key,
    s3_access_key_id)

S3_CLIENT = boto3.client(
    's3',
    aws_access_key_id=s3_access_key_id,
    aws_secret_access_key=s3_access_secret_key,
    config=Config(signature_version='s3v4')
    )

def nonpaid_marketing_spend_csv():

    '''
    Creates a csv in the temp_files dir that includes the data from the
    'Daily Marketing Spend' Google Sheet, formatted with additional null
    columns in order to load to the marketing.aggreate_spend table in redshift.
    '''

    KEEP_IN_SHEET_FILE = AIRFLOW_HOME + 'temp_files/nonpaid_marketing_spend.csv'
    S3_ARCHIVE_FILE = AIRFLOW_HOME + 'temp_files/old_marketing_spend.csv'

    spend_sheet = GoogleSheets(file_name='Daily Marketing Spend')

    sheet_vals = spend_sheet.sheet_to_df(
        sheet_name='Final sheet', index=0).values.tolist()

    sheet_vals = map(lambda row: [datetime.strptime(row[0], "%Y-%m-%d").date(),
                      row[1], row[2], float(row[3]), row[4]], sheet_vals)

    keep_in_sheet = filter(lambda row: row[0].strftime("%Y-%m") >= (datetime.today() - timedelta(days = 60)).strftime("%Y-%m"), sheet_vals)

    move_to_s3 = filter(lambda row: row[0].strftime("%Y-%m") < (datetime.today() - timedelta(days = 60)).strftime("%Y-%m"), sheet_vals)


    with open(S3_ARCHIVE_FILE, 'a') as f:
        writer = csv.writer(f)
        for row in move_to_s3:
            r = [row[0], row[1], row[2], None, None, None, row[3]]
            print r
            writer.writerow(r)

    with open(KEEP_IN_SHEET_FILE, 'w+') as f:
        writer = csv.writer(f)
        for row in keep_in_sheet:
            r = [row[0], row[1], row[2], None, None, None, row[3]]
            writer.writerow(r)

    spend_sheet.clear_sheet('Final sheet')

    spend_sheet.df_to_sheet(pd.DataFrame(keep_in_sheet,
        columns=['Date', 'Channel', 'Vendor', 'Spend', 'Notes']), 'Final sheet')

    s3.Bucket('boatsetter-warehouse').put_object(
        Key='archived_marketing_spend.csv', Body=open(S3_ARCHIVE_FILE, 'r'))

    s3.Bucket('boatsetter-warehouse').put_object(
        Key='nonpaid_marketing_spend.csv', Body=open(KEEP_IN_SHEET_FILE, 'r'))


def yesterdays_google_spend():

    yesterday = int(
        (datetime.today().date() - timedelta(days=1)).strftime("%Y%m%d"))

    yaml = AIRFLOW_HOME + '/dags/tasks/googleads.yaml'

    google_spend = map(lambda x:
            [x[0], 'cpc', 'google', x[2], x[4], x[6], x[9]/float(1000000)],
            GoogleAdsReporting('809-345-5181', yaml).daily_performance(
            yesterday, yesterday))

    with open(AIRFLOW_HOME + '/temp_files/yesterdays_google_spend.csv', 'w+') as f:
        writer = csv.writer(f)
        for row in google_spend:
            writer.writerow(row)

    keyword_spend = map(lambda x: [
              x[0] #date
            , 'cpc' #utm_medium
            , 'adwords' #utm_source
            , x[1]
            , x[2]
            , x[3]
            , x[4]
            , x[5]
            , x[6]
            , x[7]
            , x[8]
            , x[9]/float(1000000)
            , x[10]
            , x[11]
            , int(x[12])
            ],
            GoogleAdsReporting('809-345-5181', yaml).daily_keyword_performance(
                yesterday, yesterday))

    with open(AIRFLOW_HOME + '/temp_files/yesterdays_google_keyword_spend.csv', 'w+') as f:
        writer = csv.writer(f)
        for row in keyword_spend:
            writer.writerow(row)

def yesterdays_global_google_spend():

    yesterday = int((datetime.today().date() - timedelta(days=1)).strftime("%Y%m%d"))

    global_yaml = AIRFLOW_HOME + '/dags/tasks/global_googleads.yaml'

    google_spend = map(lambda x:
            [x[0], 'cpc', 'google_global', x[2], x[4], x[6], x[9]/float(1000000)],
            GoogleAdsReporting('870-732-5732', global_yaml).daily_performance(yesterday, yesterday))

    with open(AIRFLOW_HOME + '/temp_files/yesterdays_global_google_spend.csv', 'w+') as f:
        writer = csv.writer(f)
        for row in google_spend:
            writer.writerow(row)


def yesterdays_facebook_spend():

    yesterday = (
        datetime.today().date() - timedelta(days=1)).strftime("%Y-%m-%d")

    facebook_spend = map(lambda row: [
        datetime.strptime(row[0], "%Y-%m-%d").date(), 'cpc', 'facebook', row[2],
        row[4], row[6], float(row[8])],
         FacebookReporting(facebook_app_id, facebook_app_secret,
         facebook_ad_account_id).ad_level_report(yesterday, yesterday))

    with open(AIRFLOW_HOME + '/temp_files/yesterdays_facebook_spend.csv', 'w+') as f:
        writer = csv.writer(f)
        for row in facebook_spend:
            writer.writerow(row)

    FacebookReporting(facebook_app_id, facebook_app_secret,
    facebook_ad_account_id).refresh_token()


default_args = {'on_failure_callback': slack_failed_task}

dag = DAG(
    dag_id='marketing_spend_import'
    , schedule_interval='15 10 * * *'
    , start_date=datetime(2019, 06, 24)
    , catchup=False
    , default_args=default_args
    )


# GOOGLEADS IMPORT
google_spend_to_csv = PythonOperator(
    task_id='google_spend_to_csv'
    , python_callable=yesterdays_google_spend
    , dag = dag
    )

google_csv_to_s3 = LoadCsvToS3(
    task_id='load_google_csv_to_s3'
    , filename=AIRFLOW_HOME + '/temp_files/yesterdays_google_spend.csv'
    , s3_file_key='yesterdays_google_spend.csv'
    , bucket='boatsetter-warehouse'
    , dag=dag
    )

google_keyword_csv_to_s3 = LoadCsvToS3(
    task_id='load_google_keyword_csv_to_s3'
    , filename=AIRFLOW_HOME + '/temp_files/yesterdays_google_keyword_spend.csv'
    , s3_file_key='yesterdays_google_keyword_spend.csv'
    , bucket='boatsetter-warehouse'
    , dag=dag
    )

copy_google_spend_to_table = PostgresOperator(
    task_id='copy_google_csv_to_redshift'
    , sql=RedshiftCopy(
        destination_table_name='marketing.aggregate_spend'
        , s3_file='yesterdays_google_spend.csv'
        , maxerror=10).sql()
    , postgres_conn_id='redshift'
    , database='boatsetter'
    , dag=dag
    )

copy_google_keyword_spend_to_table = PostgresOperator(
    task_id='copy_google_keyword_csv_to_redshift'
    , sql=RedshiftCopy(
        destination_table_name='marketing.aggregate_spend_new'
        , s3_file='yesterdays_google_keyword_spend.csv'
        , maxerror=10).sql()
    , postgres_conn_id='redshift'
    , database='boatsetter'
    , dag=dag
    )

# GOOGLE GLOBAL SPEND IMPORT

google_global_spend_to_csv = PythonOperator(
    task_id='google_global_spend_to_csv'
    , python_callable=yesterdays_global_google_spend
    , dag = dag
    )

google_global_csv_to_s3 = LoadCsvToS3(
    task_id='load_google_global_csv_to_s3'
    , filename=AIRFLOW_HOME + '/temp_files/yesterdays_global_google_spend.csv'
    , s3_file_key='yesterdays_global_google_spend.csv'
    , bucket='boatsetter-warehouse'
    , dag=dag
    )

copy_google_global_spend_to_table = PostgresOperator(
    task_id='copy_google_global_csv_to_redshift'
    , sql=RedshiftCopy(
        destination_table_name='marketing.aggregate_spend'
        , s3_file='yesterdays_global_google_spend.csv'
        , maxerror=10).sql()
    , postgres_conn_id='redshift'
    , database='boatsetter'
    , dag=dag
    )

# FACEBOOK IMPORT
facebook_spend_to_csv = PythonOperator(
    task_id='facebook_spend_to_csv'
    , python_callable=yesterdays_facebook_spend
    , dag = dag
    )

facebook_csv_to_s3 = LoadCsvToS3(
    task_id='load_facebook_csv_to_s3'
    , filename=AIRFLOW_HOME + '/temp_files/yesterdays_facebook_spend.csv'
    , s3_file_key='yesterdays_facebook_spend.csv'
    , bucket='boatsetter-warehouse'
    , dag=dag
    )

copy_facebook_spend_to_table = PostgresOperator(
    task_id='copy_facebook_csv_to_redshift'
    , sql=RedshiftCopy(
        destination_table_name='marketing.aggregate_spend'
        , s3_file='yesterdays_facebook_spend.csv'
        , maxerror=10).sql()
    , postgres_conn_id='redshift'
    , database='boatsetter'
    , dag=dag
    )

# BING SPEND REFRESH

bing_import = BashOperator(
    task_id='import_bing'
    , bash_command = 'python /home/ubuntu/analytics/airflow_workspace/airflow_home/dags/tasks/import_bing_spend.py'
    , dag=dag
    )


# NONPAID SPEND REFRESH
nonpaid_spend_to_csv = PythonOperator(
    task_id='nonpaid_spend_to_csv'
    , python_callable=nonpaid_marketing_spend_csv
    ,dag=dag
    )


delete_existing_nonpaid = PostgresOperator(
    task_id='delete_nonpaid'
    ,sql=(
    """
    DELETE
    FROM marketing.aggregate_spend
    WHERE source NOT IN ('facebook', 'google', 'bing', 'google_global');
    """)
    ,postgres_conn_id='redshift'
    ,database='boatsetter'
    ,dag=dag
    )

copy_nonpaid_to_table = PostgresOperator(
    task_id='copy_nonpaid_csv_to_redshift'
    , sql=RedshiftCopy(
        destination_table_name='marketing.aggregate_spend'
        , s3_file='nonpaid_marketing_spend.csv'
        , maxerror=10).sql()
    , postgres_conn_id='redshift'
    , database='boatsetter'
    , dag=dag
    )

copy_nonpaid_archive_to_table = PostgresOperator(
    task_id='copy_nonpaid_archive_to_table'
    , sql=RedshiftCopy(
        destination_table_name='marketing.aggregate_spend'
        , s3_file='archived_marketing_spend.csv'
        , maxerror=10).sql()
    , postgres_conn_id='redshift'
    , database='boatsetter'
    , dag=dag
    )

google_spend_to_csv >> google_csv_to_s3 >> google_keyword_csv_to_s3 >> copy_google_spend_to_table
copy_google_spend_to_table >> copy_google_keyword_spend_to_table >> facebook_spend_to_csv >> facebook_csv_to_s3
facebook_csv_to_s3 >> copy_facebook_spend_to_table >> google_global_spend_to_csv >> google_global_csv_to_s3
google_global_csv_to_s3 >> copy_google_global_spend_to_table >> bing_import
bing_import >> nonpaid_spend_to_csv >> delete_existing_nonpaid
delete_existing_nonpaid >> copy_nonpaid_to_table >> copy_nonpaid_archive_to_table
