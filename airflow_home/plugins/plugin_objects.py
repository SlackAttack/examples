import psycopg2
import boto3
from botocore.client import Config
from airflow_home.credential_vars import s3_access_key_id, s3_access_secret_key, redshift_conn_string


class CopyFromS3():

    def __init__(self, destination_table_name, s3_file, maxerror):
        self.destination_table_name = destination_table_name
        self.s3_file = s3_file
        self.maxerror = maxerror

    def sql(self):
        sql = """
        COPY %s
        FROM 's3://nextstepetlprod/%s'
        access_key_id '%s'
        secret_access_key '%s'
        CSV
        IGNOREHEADER 1
        DELIMITER ','
        NULL 'None'
        BLANKSASNULL
        EMPTYASNULL
        MAXERROR %s;
        """ % (self.destination_table_name, self.s3_file, s3_access_key_id, s3_access_secret_key, self.maxerror)
        return sql

    def execute(self):
        cursor.execute(self.sql())
        conn.commit()


class PostgresConnection():

    def __init__(self, connection_string):
        self.connection_string = connection_string

    def connect_cursor(self):
        conn = psycopg2.connect(redshift_conn_string)
        cursor = conn.cursor()


s3 = boto3.resource(
    's3',
    aws_access_key_id=s3_access_key_id,
    aws_secret_access_key=s3_access_secret_key,
    config=Config(signature_version='s3v4')
)
