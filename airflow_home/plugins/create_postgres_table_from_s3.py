import logging
import csv
import psycopg2
import boto3
from botocore.client import Config
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.plugins_manager import AirflowPlugin

log = logging.getLogger(__name__)


class CreatePostgresTableFromS3Definitions(BaseOperator):

    @apply_defaults
    def __init__(self,
                 bucket,
                 source_table,
                 table_def_s3_key,
                 postgres_conn_id,
                 schema,
                 *args,
                 **kwargs):

        super(CreatePostgresTableFromS3Definitions, self).__init__(*args, **kwargs)
        self.bucket = bucket
        self.table_def_s3_key = table_def_s3_key
        self.postgres_conn_id = postgres_conn_id
        self.source_table = source_table
        self.schema = schema

    def execute(self, context):
        data = self.get_from_s3()
        self.create_new_tables(self.source_table, data, self.schema)

    def get_from_s3(self):

        s3 = boto3.resource(
            's3',
            aws_access_key_id=str(Variable.get("s3_access_key_id")),
            aws_secret_access_key=str(Variable.get("s3_access_secret_key")),
            config=Config(signature_version='s3v4')
        )

        content_object = s3.Object(self.bucket, self.table_def_s3_key)
        file_content = content_object.get()['Body'].read().decode('utf-8').splitlines(True)
        reader = csv.reader(file_content)
        return reader

    def create_new_tables(self, source_table, column_file, schema):

        column_list = []
        for x in column_file:
            column_list.append([x[0], x[1]])

        postgres = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        conn = postgres.get_conn()
        cursor = conn.cursor()

        statement = 'CREATE TABLE %s.%s (%s %s' % (schema, source_table, column_list[1][0], column_list[1][1])
        for x in column_list[2:]:
            statement = statement + ', \"' + '%s' % (x[0]) + '\"' + ' %s' % (x[1])

        statement = str(statement) + ');'
        cursor.execute("""%s""" % statement)
        conn.commit()
        conn.close()


class CreatePostgresTableFromS3DefinitionsPlugin(AirflowPlugin):
    name = "create_postgres_table_from_s3"
    operators = [CreatePostgresTableFromS3Definitions]
