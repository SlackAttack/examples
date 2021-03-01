import csv
from airflow.utils.decorators import apply_defaults
from airflow.plugins_manager import AirflowPlugin
from airflow.models import Variable
from airflow.models import BaseOperator
import boto3
from botocore.client import Config


class LoadCsvToS3(BaseOperator):

    @apply_defaults
    def __init__(self,
                 filename,
                 s3_file_key,
                 bucket,
                 *args,
                 **kwargs):
        super(LoadCsvToS3, self).__init__(*args, **kwargs)
        self.filename = filename
        self.s3_file_key = s3_file_key
        self.bucket = bucket

    def execute(self, context):
        csv_data = open("%s" % self.filename, 'rb')

        s3 = boto3.resource(
            's3',
            aws_access_key_id=str(Variable.get("s3_access_key_id")),
            aws_secret_access_key=str(Variable.get("s3_access_secret_key")),
            config=Config(signature_version='s3v4')
        )

        s3.Bucket(self.bucket).put_object(Key="%s" % self.s3_file_key, Body=csv_data)


class LoadCsvToS3Plugin(AirflowPlugin):
    name = "load_csv_to_s3"
    operators = [LoadCsvToS3]
