from google.cloud import bigquery
from google.cloud.storage import Blob
from google.cloud.exceptions import NotFound
import subprocess

class BaseSchema:
    def __init__(self, bq_client, st_client, dataset, bucket, date):
        self.bq_client = bq_client
        self.st_client = st_client
        self.dataset = dataset
        self.bucket = bucket
        self.date = date

    def table_exists(self):
        try:
            table_ref = self.dataset.table(self.get_table())
            self.bq_client.get_table(table_ref)
            return True
        except NotFound as e:
            return False

    def storage_file_exists(self, file_name):
        check = subprocess.call(['gsutil', 'ls', self.get_location()])
        if check == 1:
            return False
        return True

    def load_data(self, skip_table):
        if not self.storage_file_exists(self.get_file_name()):
            return False
        table_ref = self.create_table(skip_table)
        job_config = bigquery.LoadJobConfig()
        # BigQuery 書き込みオペレーションによってテーブルを作成するかどうかを制御指定。
        # CREATE_NEVER: テーブルを作成しないよう指定します.
        job_config.create_disposition = 'NEVER'
        job_config.skip_leading_rows = 1
        # CloudStorageから取得するファイル名は「.gz」ファイルでも問題ありません。
        job_config.source_format = 'CSV'
        job_config.write_disposition = 'WRITE_TRUNCATE'

        return self.bq_client.load_table_from_uri(self.get_location(), table_ref, job_config=job_config)

    def get_location(self):
        return 'gs://{}/{}'.format(self.bucket, self.get_file_name())

    def create_table(self, skip):
        table_ref = self.dataset.table(self.get_table())
        if skip == True:
            return table_ref
        table = bigquery.Table(table_ref, schema=self.get_schema())
        table.partitioning_type = 'DAY'
        self.bq_client.create_table(table)

        return table_ref

    def get_table(self):
        return '{}{}'.format(self.table, self.date)
