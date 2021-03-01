import logging
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.plugins_manager import AirflowPlugin
import pandas as pd
from airflow_home.plugins.plugin_objects import s3

log = logging.getLogger(__name__)


class PostgresTableDefinitionsToS3(BaseOperator):

    @apply_defaults
    def __init__(self
                 , bucket
                 , s3_file_key
                 , table_name
                 , postgres_conn_id
                 , *args
                 , **kwargs):
        super(PostgresTableDefinitionsToS3, self).__init__(*args, **kwargs)
        self.bucket = bucket
        self.s3_file_key = s3_file_key
        self.table_name = table_name
        self.postgres_conn_id = postgres_conn_id

    def execute(self, context):
        # query postgres for column name, and data type of column
        postgres = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        conn = postgres.get_conn()
        cursor = conn.cursor()
        cursor.execute("""SELECT
                            column_name
                            ,CASE
                        	    WHEN data_type = 'character varying' THEN concat('varchar', '(',(CASE WHEN character_maximum_length IS NULL then 255 ELSE character_maximum_length END),')')
                        		WHEN data_type = 'integer' THEN 'int'
                        	    WHEN data_type = 'text' THEN concat('varchar','(', 10000, ')')
                        	    WHEN data_type = 'uuid' THEN concat('varchar','(', 255, ')')
                        	    WHEN data_type = 'name' THEN concat('varchar','(', 255, ')')
                        	    WHEN data_type = 'money' THEN concat('varchar','(', 255, ')')
                        	    WHEN data_type = 'numeric' AND numeric_precision IS NULL THEN 'double precision'
                        	    WHEN data_type = 'numeric' AND numeric_precision IS NOT NULL THEN concat('numeric(',numeric_precision,',',numeric_scale,')')
                        		ELSE data_type
                            END
                            FROM information_schema.columns
                            WHERE table_name = '%s'
                                AND data_type NOT IN ('ARRAY', 'USER-DEFINED', 'json')
                                AND table_schema='public'
                            ORDER BY ordinal_position""" % self.table_name)
        data = cursor.fetchall()
        data = map(lambda x: [x[0], x[1]], data)
        self.export_to_s3(data)
        log.info("Exported %s table Definitions CSV to s3" % self.table_name)

    def export_to_s3(self, x):
        pd.DataFrame(x).to_csv(
            "/home/ubuntu/airflow/airflow_home/temp_files/%s" % str(self.s3_file_key), index=False)
        csv_data = open("/home/ubuntu/airflow/airflow_home/temp_files/%s" % str(self.s3_file_key),
                        'rb')
        s3.Bucket(self.bucket).put_object(Key=self.s3_file_key, Body=csv_data)


class PostgresTableDefinitionsToS3Plugin(AirflowPlugin):
    name = "postgres_table_defs_to_s3"
    operators = [PostgresTableDefinitionsToS3]





