import io
from httplib2 import Http
from oauth2client import file, client, tools
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.discovery import build
from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin


class GoogleSheetToCSV(BaseOperator):

    def __init__(self, filename, file_id, *args, **kwargs):

        super(GoogleSheetToCSV, self).__init__(*args, **kwargs)
        self.filename = filename
        self.file_id = file_id

    def execute(self, context):
        self.func(self.filename, self.file_id)

    def func(self, filename, file_id):
        scopes = 'https://www.googleapis.com/auth/drive'
        store = file.Storage('/home/ubuntu/airflow/airflow_home/google_drive_api/token.json')
        creds = store.get()
        if not creds or creds.invalid:
            flow = client.flow_from_clientsecrets('/home/ubuntu/airflow/airflow_home/google_drive_api/credentials.json',
                                                  scopes)
            creds = tools.run_flow(flow, store)
        service = build('drive', 'v3', http=creds.authorize(Http()))

        # Replace this file_id with your spreadsheet file id
        file_id = '%s' % file_id
        request = service.files().export_media(fileId=file_id, mimeType='text/csv')

        fh = io.FileIO('/home/ubuntu/airflow/airflow_home/temp_files/%s' % filename, 'wb')
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()


class GSheetToCSVPlugin(AirflowPlugin):
    name = "gsheet_to_csv"
    operators = [GoogleSheetToCSV]
