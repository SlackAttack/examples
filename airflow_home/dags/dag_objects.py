from airflow_home.credential_vars import s3_access_key_id, s3_access_secret_key
import psycopg2
import boto3
from botocore.client import Config
from gspread_pandas import Spread, Client
import pandas as pd



# Copy From S3 To Redshift Table
class RedshiftCopy(object):

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
        ACCEPTINVCHARS
        IGNOREHEADER 1
        DELIMITER ','
        NULL 'None'
        BLANKSASNULL
        EMPTYASNULL
        MAXERROR %s;
        """ % (self.destination_table_name, self.s3_file
                             , s3_access_key_id, s3_access_secret_key, self.maxerror)
        return sql


# S3 Connection


s3 = boto3.resource(
    's3',
    aws_access_key_id=s3_access_key_id,
    aws_secret_access_key=s3_access_secret_key,
    config=Config(signature_version='s3v4')
)

s3Client = boto3.client(
    's3',
    aws_access_key_id=s3_access_key_id,
    aws_secret_access_key=s3_access_secret_key
)


class GoogleSheets(object):

    def __init__(self, file_name):
        self.file_name = file_name
        """Authenication from oauth token from "My First Project", project in google developer console - owned by 
           reporting@nextstep.careers user"""
        self.spread = Spread(('1096395197574-0lbfvsdbd5crh7hnlgmm4asll1u3f0g5'
                              '.apps.googleusercontent.com'), self.file_name)

    def to_csv(self, sheet_name, output_file_path):
        df = self.spread.sheet_to_df(
            sheet=sheet_name
            , index=0
        )

        df.to_csv(output_file_path, index=False)

    def csv_to_sheet(self, csv_path, start_tuple, output_sheet_name):
        df = pd.read_csv(csv_path)

        self.spread.df_to_sheet(
            df
            , start=start_tuple
            , sheet=output_sheet_name
            , index=False
        )

    def clear_sheet(self, sheet_name):
        # self.spread.open_sheet(sheet=sheet_name)
        self.spread.clear_sheet(sheet=sheet_name, rows=0, cols=0)

    def df_to_sheet(self, df, sheet_name):
        self.spread.df_to_sheet(df=df, sheet=sheet_name, index=False
                                , headers=True, replace=True
                                , freeze_headers=True)

    def sheet_to_df(self, sheet_name, index=1, header_rows=1, start_row=1):
        return self.spread.sheet_to_df(sheet=sheet_name
                                       , index=index, header_rows=header_rows
                                       , start_row=start_row)


# Used to check if source and destination tables are the same


class TableColumns(object):

    def __init__(self, table, schema):
        self.table = table
        self.schema = schema

    def redshift_sql(self):
        sql = (
                """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name='%s'
        AND table_schema='%s'
        """ % (self.table, self.schema))
        return sql

    def zeus_sql(self):
        sql = (
                """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name='%s'
        AND data_type NOT IN ('ARRAY', 'USER-DEFINED', 'json')
        AND table_schema='%s'
        """ % (self.table, self.schema))
        return sql
