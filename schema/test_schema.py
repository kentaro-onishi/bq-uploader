from base_schema import BaseSchema
from google.cloud import bigquery

class TestSchema(BaseSchema):
    table = 'table_test_schema'

    def get_file_name(self):
        return 'table_test_schema{}*'.format(self.date)

    def get_schema(self):
        return [
            bigquery.SchemaField('HOGE', 'STRING'),
            bigquery.SchemaField('FUGA', 'STRING'),
            bigquery.SchemaField('PIYO', 'STRING')
        ]
