import os
import json

AIRFLOW_HOME = os.getenv('AIRFLOW_HOME', '/home/ubuntu/airflow/airflow_home')

# load in the different connections variables from the config file
with open(AIRFLOW_HOME + '/config.json', 'r') as c:
    config = json.load(c)

# Redshift
REDSHIFT_CONN_STRING = config.get("REDSHIFT_CONN_STRING")
REDSHIFT_PW = config.get("REDSHIFT_PW")
REDSHIFT_USER = config.get("REDSHIFT_USER")
REDSHIFT_HOST = config.get("REDSHIFT_HOST")
SQL_ALCH_REDSHIFT_CONN_STRING = config.get("SQL_ALCH_REDSHIFT_CONN_STRING")
ZEUS_CONN_STRING = config.get("ZEUS_CONN_STRING")
S3_ACCESS_KEY_ID = config.get("S3_ACCESS_KEY_ID")
S3_ACCESS_SECRET_KEY = config.get("S3_ACCESS_SECRET_KEY")
GMAIL_REPORTING_UID = config.get("GMAIL_REPORTING_UID")
GMAIL_REPORTING_PWD = config.get("GMAIL_REPORTING_PWD")

