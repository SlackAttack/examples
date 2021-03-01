import logging
import psycopg2
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.plugins_manager import AirflowPlugin
import csv

log = logging.getLogger(__name__)


class PostgresToCsv(BaseOperator):

    @apply_defaults
    def __init__(self,
                 sql,
                 csv_path,
                 csv_filename,
                 postgres_conn_id,
                 *args,
                 **kwargs):
        super(PostgresToCsv, self).__init__(*args, **kwargs)
        self.sql = sql
        self.csv_path = csv_path
        self.csv_filename = csv_filename
        self.postgres_conn_id = postgres_conn_id

    def execute(self, context):
        postgres = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        conn = postgres.get_conn()
        cursor = conn.cursor()
        cursor.execute(self.sql)
        data = cursor.fetchall()

        self.create_csv(data, self.csv_path, self.csv_filename)

    def create_csv(self, data, csv_path, csv_filename):
        with open(csv_path + csv_filename, 'wt', encoding='utf-8') as myfile:
            wr = csv.writer(myfile)
            wr.writerows(data)


class PostgresToCsvPlugin(AirflowPlugin):
    name = "postgres_to_csv"
    operators = [PostgresToCsv]
