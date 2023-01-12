import math
import time
from retry import retry
from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core import exceptions
import google.auth

from gcloud_connectors.logger import EmptyLogger


class BigQueryConnector:
    def __init__(self, project_id, confs_path=None, auth_type='service_account', json_keyfile_dict=None, logger=None):
        self.confs_path = confs_path
        self.json_keyfile_dict = json_keyfile_dict
        self.auth_type = auth_type
        self.project_id = project_id
        self.logger = logger if logger is not None else EmptyLogger()
        # authorization boilerplate code
        # os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'{}/{}'.format(os.getcwd(), confs_path)

        if self.confs_path is not None or self.json_keyfile_dict is not None:
            if self.confs_path is not None:
                self.creds = service_account.Credentials.from_service_account_file(
                    self.confs_path,
                )
            elif self.json_keyfile_dict is not None:
                self.creds = service_account.Credentials.from_service_account_info(
                    self.json_keyfile_dict
                )

        else:
            self.creds, project = google.auth.default(
                scopes=[
                    "https://www.googleapis.com/auth/drive",
                    "https://www.googleapis.com/auth/bigquery",
                ]
            )
        self.service = bigquery.Client(project=self.project_id, credentials=self.creds)
        self.destination = None
        self.results_per_page = None
        self.num_pages = None
        self.index = None
        self.next_token = None

    @staticmethod
    def pd_cast_dtypes(df, table_dtypes):
        """
        :param df: pandas DataFrame coming from BigQuery sql
        :param table_dtypes: pandas dtypes
        :return: pandas DataFrame casted
        """
        table_dtypes = {col: d_type for col, d_type in table_dtypes.items() if col in df.columns}
        try:
            df = df.astype(table_dtypes)
        except Exception as e:
            print(e)
            for col, d_type in table_dtypes.items():
                if col in df.columns:
                    try:
                        if d_type == 'integer':
                            d_type = int
                        if d_type == 'float':
                            d_type = float
                        if d_type == 'string' or d_type == 'category' or d_type == str:
                            d_type = str
                        if d_type in (str, float, int):
                            if d_type in (float, int):
                                pass
                                # df[col] = pd.to_numeric(df[col], errors='coerce')
                            df[col] = df[col].astype(d_type)
                        if d_type in ('boolean', 'bool'):
                            try:
                                df[col] = df[col].astype(int).fillna(False)
                            except Exception as e:
                                pass
                            df[col] = df[col].replace({1: True, 0: False})
                    except Exception as e:
                        print('No casting for {} into {} from {}'.format(col, d_type, df[col].dtype))
                        print(e)
        return df

    def pd_get_dtypes(self, dataset, table):
        """
        :param dataset: BigQuery dataset name
        :param table: BigQuery table name
        :return: dict of {col1: dtype1, col2: dtype1, col3: dtype3....}
        """
        return self.get_pandas_dtypes(dataset, table)

    def get_pandas_dtypes(self, dataset, table):
        """
        :param dataset: BigQuery dataset name like bigquery-public-data.bitcoin_blockchain
        :param table: BigQuery table name like blocks
        :return: dict of column types as {col1: dtype1, col2: dtype1, col3: dtype3....}
        """
        table_ref = '{}.{}'.format(dataset, table)
        table = self.service.get_table(table_ref)
        schema_dtypes = {}
        for schema in table.schema:
            if schema.field_type in ['STRING']:
                type_x = str
            elif schema.field_type in ['DATE', 'TIMESTAMP']:
                type_x = 'datetime64[ns]'
            elif schema.field_type in ['FLOAT']:
                type_x = schema.field_type.lower()
            elif schema.field_type in ['INTEGER']:
                type_x = 'int'
            elif schema.field_type in ['BOOLEAN']:
                type_x = 'bool'
            else:
                raise AttributeError('Unknown type {} for {}'.format(schema.field_type, schema.name))
            schema_dtypes[schema.name] = type_x
        return schema_dtypes

    def pd_execute(self, query, progress_bar_type=None, bqstorage_enabled=False):
        """
        :param query: sql query
        :param progress_bar_type:
        :param bqstorage_enabled: whether to user or not bqstorage to download results more quickly
        :return: pandas DataFrame from BigQuery sql execution
        """
        del progress_bar_type
        if bqstorage_enabled is True:
            return (
                self.service.query(query)
                    .result()
                    .to_dataframe(create_bqstorage_client=True)
            )
        else:
            return (
                self.service.query(query)
                    .result()
                    .to_dataframe()
            )

    def clear_chunked_variables(self):
        self.destination = None
        self.results_per_page = None
        self.num_pages = None
        self.index = None
        self.next_token = None

    def has_next(self):
        if self.index != self.num_pages:
            return True
        else:
            self.clear_chunked_variables()
            return False

    @retry(exceptions.NotFound, tries=3, delay=2)
    def pd_execute_chunked(self, query, progress_bar_type=None, bqstorage_enabled=False, first_run=True,
                           results_per_page=10, sleep_time=None):

        if first_run:
            query_job = self.service.query(query)

            while query_job.done() is not True:
                self.logger.info("waiting for job completion")

            destination = query_job.destination
            try:
                destination = self.service.get_table(destination)
            except exceptions.NotFound:
                if sleep_time:
                    time.sleep(sleep_time)
                destination = self.service.get_table(destination)
            self.destination = destination
            self.results_per_page = results_per_page
            self.num_pages = math.ceil(float(destination.num_rows / results_per_page))
            self.index = 0
            self.next_token = None

        if self.next_token:
            rows = self.service.list_rows(self.destination,
                                          max_results=self.results_per_page,
                                          page_token=self.next_token)
        else:
            rows = self.service.list_rows(self.destination,
                                          max_results=self.results_per_page)

        if self.index < self.num_pages:

            df = rows.to_dataframe()
            self.index += 1
            self.next_token = rows.next_page_token

            return df

        else:
            return None
