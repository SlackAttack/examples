from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.bash_operator import BaseOperator
from airflow.models import Variable
from airflow.plugins_manager import AirflowPlugin
import logging
import csv
from airflow.utils.decorators import apply_defaults

log = logging.getLogger(__name__)


class CopyPostgresTableFromS3(BaseOperator):

    @apply_defaults
    def __init__(self,
                 table_name,
                 filename,
                 postgres_conn_id,
                 database,
                 *args, **kwargs):
        super(CopyPostgresTableFromS3, self).__init__(*args, **kwargs)
        self.table_name = table_name
        self.filename = filename
        self.postgres_conn_id = postgres_conn_id
        self.database = database

    def execute(self, context):
        PostgresOperator(sql="""copy %s
                                FROM '{{ params.source }}'
                                ACCESS_KEY_ID '{{ params.access_key}}'
                                SECRET_ACCESS_KEY '{{ params.secret_key }}'
                                ACCEPTINVCHARS
                                IGNOREHEADER 1
                                CSV
                                BLANKSASNULL
                                EMPTYASNULL
                                MAXERROR 100""" % self.table_name,
                         postgres_conn_id=self.postgres_conn_id,
                         database=self.database,
                         params={
                             'source': 's3://nextstepetlprod/%s' % self.filename,
                             'access_key': str(Variable.get("s3_access_key_id")),
                             'secret_key': str(Variable.get("s3_access_secret_key"))
                         }
                         )


class CopyDataToPostgresPlugin(AirflowPlugin):
    name = "copy_data_to_postgres"
    operators = [CopyPostgresTableFromS3]
