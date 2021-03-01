import logging
import csv
import psycopg2
import boto3
from botocore.client import Config
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.plugins_manager import AirflowPlugin
from airflow_home.plugins.plugin_objects import s3

log = logging.getLogger(__name__)


class PostgresToS3(BaseOperator):

    @apply_defaults
    def __init__(self,
                 table_name,
                 bucket,
                 table_def_s3_key,
                 postgres_conn_id,
                 *args,
                 **kwargs):

        super(PostgresToS3, self).__init__(*args, **kwargs)
        self.table_name = table_name
        self.bucket = bucket
        self.table_def_s3_key = table_def_s3_key
        self.postgres_conn_id = postgres_conn_id

    def execute(self, context):
        data = self.get_from_s3()
        self.load_data_to_s3(self.table_name, data)

    def get_from_s3(self):

        content_object = s3.Object(self.bucket, self.table_def_s3_key)
        file_content = content_object.get()['Body'].read().decode('utf-8').splitlines(True)
        reader = csv.reader(file_content)
        return reader

    def load_data_to_s3(self, table_name, data):

        column_names = []
        for x in data:
            column_names.append(x[0])

        postgres = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        conn = postgres.get_conn()
        cursor = conn.cursor()

        statement = 'SELECT %s.%s' % (table_name, column_names[1])
        for x in column_names[2:]:
            statement = statement + ', %s.%s' % (table_name, x)

        statement = statement + ' FROM %s;' % table_name

        cursor.execute("""%s""" % statement)
        conn.commit()
        table_data = cursor.fetchall()

        with open('/home/ubuntu/airflow/airflow_home/temp_files/%s_%s_table.csv' % (
                self.postgres_conn_id, table_name), 'wt') as myfile:
            wr = csv.writer(myfile)
            wr.writerows(table_data)

        csv_data = open("/home/ubuntu/airflow/airflow_home/temp_files/%s_%s_table.csv" % (
            self.postgres_conn_id, table_name), 'rb')

        s3.Bucket(self.bucket).put_object(Key="%s_%s_table.csv" % (self.postgres_conn_id, table_name), Body=csv_data)


class PostgresToS3Plugin(AirflowPlugin):
    name = "postgres_data_to_s3"
    operators = [PostgresToS3]
